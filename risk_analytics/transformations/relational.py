"""Relational-style transformation components (join, lookup)."""
from __future__ import annotations

from collections.abc import Iterable
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from risk_analytics.transformations.common import ComponentExecutionError, dataset, required_str
from risk_analytics.transformations.expressions import apply_select


def run_join(datasets: dict[str, DataFrame], step: dict[str, Any], config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Apply one or more joins and retain an anti-join reconciliation view.

    The first join is used to identify left-side records without a match. This
    gives pipeline consumers an immediately useful exception dataset while still
    allowing multi-join output to be built as a normal Spark DataFrame flow.
    """
    left_name = required_str(step, "input")
    joins = step.get("joins", [])
    if not joins:
        raise ComponentExecutionError("Join component requires at least one join entry.")

    left = dataset(datasets, left_name)
    output = left

    for join_spec in joins:
        right_name = required_str(join_spec, "dataset")
        right = dataset(datasets, right_name)
        how = join_spec.get("how", "inner")
        on_clause = build_join_clause(output, right, join_spec.get("on"))
        output = output.join(right, on_clause, how)

    if "select" in step:
        output = apply_select(output, step["select"], config)

    first_join = joins[0]
    first_right = dataset(datasets, required_str(first_join, "dataset"))
    first_on = build_join_clause(left, first_right, first_join.get("on"))
    unused = left.join(first_right, first_on, "left_anti")
    used = output
    return output, used, unused


def run_lookup(datasets: dict[str, DataFrame], step: dict[str, Any], _config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Perform a deterministic left lookup and split matched from unmatched rows.

    Lookup inputs require an explicit ordering rule because duplicate reference
    keys are common in real data. Ranking before joining makes the selected match
    predictable rather than dependent on Spark's physical execution order.
    """
    left = dataset(datasets, required_str(step, "input"))
    right = dataset(datasets, required_str(step, "lookup"))

    keys = step.get("keys")
    if not keys:
        raise ComponentExecutionError("Lookup component requires non-empty 'keys'.")

    order_by = step.get("order_by")
    if not order_by:
        raise ComponentExecutionError("Lookup component requires non-empty 'order_by' for deterministic first match.")

    values = step.get("values", [])
    key_pairs = normalize_key_pairs(keys)
    right_partition = [F.col(pair[1]) for pair in key_pairs]
    right_order = order_columns(order_by, default_desc=False)

    # Choose exactly one reference row per lookup key before enriching the left set.
    ranked = right.withColumn("__lookup_rank", F.row_number().over(Window.partitionBy(*right_partition).orderBy(*right_order)))
    ranked = ranked.filter(F.col("__lookup_rank") == F.lit(1)).drop("__lookup_rank")
    ranked = ranked.withColumn("__lookup_matched", F.lit(1))

    left_alias = "__left"
    right_alias = "__right"
    left_aliased = left.alias(left_alias)
    ranked_aliased = ranked.alias(right_alias)
    joined = left_aliased.join(ranked_aliased, build_join_clause(left_aliased, ranked_aliased, keys), "left")

    left_columns = [F.col(f"{left_alias}.{name}").alias(name) for name in left.columns]
    value_columns = []
    for value_spec in values:
        right_col = required_str(value_spec, "right")
        output_name = value_spec.get("name", right_col)
        value_columns.append(F.col(f"{right_alias}.{right_col}").alias(output_name))

    if value_columns:
        output = joined.select(*left_columns, *value_columns, F.col(f"{right_alias}.__lookup_matched").alias("__lookup_matched"))
    else:
        output = joined

    used = output.filter(F.col("__lookup_matched").isNotNull()).drop("__lookup_matched")
    unused = output.filter(F.col("__lookup_matched").isNull()).drop("__lookup_matched")
    output = output.drop("__lookup_matched")
    return output, used, unused


def build_join_clause(left: DataFrame, right: DataFrame, on_spec: Any):
    """Normalize supported YAML key syntaxes into Spark join conditions."""
    if on_spec is None:
        common = [column for column in left.columns if column in set(right.columns)]
        if len(common) == 1:
            return common
        if len(common) > 1:
            raise ComponentExecutionError(
                "Join specification is missing 'on' and multiple common columns exist; "
                f"provide an explicit clause (common columns: {', '.join(common)})."
            )
        raise ComponentExecutionError("Join specification requires a non-empty 'on' clause.")

    if isinstance(on_spec, list) and not on_spec:
        raise ComponentExecutionError("Join specification requires a non-empty 'on' clause.")

    if isinstance(on_spec, list):
        if all(isinstance(item, str) for item in on_spec):
            return on_spec
        if all(isinstance(item, dict) for item in on_spec):
            clauses = [
                left[str(item["left"])] == right[str(item["right"])]
                for item in on_spec
            ]
            return reduce(lambda first, second: first & second, clauses)

    if isinstance(on_spec, dict):
        clauses = [left[str(left_col)] == right[str(right_col)] for left_col, right_col in on_spec.items()]
        return reduce(lambda first, second: first & second, clauses)

    raise ComponentExecutionError("Join 'on' must be list[str], list[{left,right}], or mapping {left:right}.")


def normalize_key_pairs(keys: Any) -> list[tuple[str, str]]:
    if isinstance(keys, dict):
        return [(str(left_col), str(right_col)) for left_col, right_col in keys.items()]
    if isinstance(keys, list):
        pairs = []
        for item in keys:
            if isinstance(item, str):
                pairs.append((item, item))
            elif isinstance(item, dict):
                pairs.append((str(item["left"]), str(item["right"])))
            else:
                raise ComponentExecutionError("Lookup keys list entries must be strings or {left,right} objects.")
        return pairs
    raise ComponentExecutionError("Lookup keys must be mapping or list.")


def order_columns(order_by: Iterable[Any], default_desc: bool) -> list[Any]:
    output = []
    for item in order_by:
        if isinstance(item, str):
            output.append(F.col(item).desc() if default_desc else F.col(item).asc())
            continue
        if isinstance(item, dict):
            column = required_str(item, "column")
            desc = bool(item.get("desc", default_desc))
            output.append(F.col(column).desc() if desc else F.col(column).asc())
            continue
        raise ComponentExecutionError("Order-by entries must be strings or {column, desc} objects.")
    return output
