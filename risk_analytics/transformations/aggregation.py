"""Aggregation and shape-changing transformation components."""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, Window, functions as F

from risk_analytics.transformations.common import ComponentExecutionError, dataset, required_str
from risk_analytics.transformations.relational import order_columns


def run_rollup(datasets: dict[str, DataFrame], step: dict[str, Any], _config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Aggregate an input dataset using a constrained, readable YAML grammar."""
    source = dataset(datasets, required_str(step, "input"))
    keys = step.get("group_by")
    if not keys:
        raise ComponentExecutionError("Rollup component requires non-empty 'group_by'.")

    aggregations = step.get("aggregations")
    if not aggregations:
        raise ComponentExecutionError("Rollup component requires non-empty 'aggregations'.")

    expressions = []
    for agg in aggregations:
        name = required_str(agg, "name")
        op = required_str(agg, "op").lower()
        column = agg.get("col")

        if op == "count" and column is None:
            expr = F.count(F.lit(1))
        elif op == "count_distinct":
            expr = F.countDistinct(F.col(required_str(agg, "col")))
        elif op == "sum":
            expr = F.sum(F.col(required_str(agg, "col")))
        elif op == "min":
            expr = F.min(F.col(required_str(agg, "col")))
        elif op == "max":
            expr = F.max(F.col(required_str(agg, "col")))
        elif op == "avg":
            expr = F.avg(F.col(required_str(agg, "col")))
        elif op == "first":
            expr = F.first(F.col(required_str(agg, "col")), ignorenulls=True)
        else:
            raise ComponentExecutionError(f"Unsupported rollup aggregation op '{op}'.")
        expressions.append(expr.alias(name))

    output = source.groupBy(*keys).agg(*expressions)
    return output, source, source.limit(0)


def run_normalize(datasets: dict[str, DataFrame], step: dict[str, Any], _config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Expand a record into a controlled number of copies for normalization use cases."""
    source = dataset(datasets, required_str(step, "input"))
    factor_col = step.get("n_column")
    factor_literal = step.get("n")

    if factor_col is None and factor_literal is None:
        raise ComponentExecutionError("Normalize component requires either 'n' or 'n_column'.")

    if factor_col is not None:
        multiplier = F.coalesce(F.col(str(factor_col)).cast("int"), F.lit(0))
    else:
        multiplier = F.lit(int(factor_literal)).cast("int")

    # Spark sequence/explode keeps expansion distributed instead of collecting rows.
    output = source.withColumn("__normalize_n", multiplier)
    output = output.withColumn("__normalize_rep", F.explode(F.sequence(F.lit(1), F.col("__normalize_n"))))
    output = output.drop("__normalize_n", "__normalize_rep")
    return output, source, source.limit(0)


def run_dedup(datasets: dict[str, DataFrame], step: dict[str, Any], _config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Keep unique, first, or last records while returning duplicates explicitly.

    ``first`` and ``last`` require a declared order-by clause so the survivor is
    a business rule visible in YAML, never an accidental consequence of input
    order or Spark partitioning.
    """
    source = dataset(datasets, required_str(step, "input"))
    mode = required_str(step, "mode").lower()
    keys = step.get("keys")
    if not keys:
        raise ComponentExecutionError("Dedup component requires non-empty 'keys'.")

    if mode == "unique":
        counts = source.groupBy(*keys).count()
        unique_keys = counts.filter(F.col("count") == F.lit(1)).drop("count")
        output = source.join(unique_keys, keys, "inner")
        duplicates = source.join(unique_keys, keys, "left_anti")
        return output, output, duplicates

    if mode not in {"first", "last"}:
        raise ComponentExecutionError("Dedup mode must be one of: unique, first, last.")

    order_by = step.get("order_by")
    if not order_by:
        raise ComponentExecutionError("Dedup mode 'first' or 'last' requires non-empty 'order_by'.")

    default_desc = mode == "last"
    order_columns_list = order_columns(order_by, default_desc=default_desc)
    window = Window.partitionBy(*keys).orderBy(*order_columns_list)
    ranked = source.withColumn("__dedup_rank", F.row_number().over(window))
    output = ranked.filter(F.col("__dedup_rank") == F.lit(1)).drop("__dedup_rank")
    duplicates = ranked.filter(F.col("__dedup_rank") > F.lit(1)).drop("__dedup_rank")
    return output, output, duplicates
