"""Filter and reformat transformation components."""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame

from risk_analytics.transformations.common import ComponentExecutionError, dataset, required_str
from risk_analytics.transformations.expressions import apply_select, build_condition, build_expression


def run_filter(datasets: dict[str, DataFrame], step: dict[str, Any], config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Apply a declarative predicate and retain both accepted and rejected rows."""
    source = dataset(datasets, required_str(step, "input"))
    condition_spec = step.get("condition")
    if condition_spec is None:
        raise ComponentExecutionError("Filter component requires 'condition'.")

    condition = build_condition(condition_spec, config)
    used = source.filter(condition)
    unused = source.filter(~condition)
    return used, used, unused


def run_reformat(datasets: dict[str, DataFrame], step: dict[str, Any], config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Add, rename, remove, and select columns using ordered YAML operations.

    Reformatting is intentionally sequential: a later operation may reference a
    column created earlier in the same step, mirroring an easily explainable
    data-preparation workflow.
    """
    output = dataset(datasets, required_str(step, "input"))

    for op in step.get("operations", []):
        op_name = op.get("op", "add_column").lower()
        if op_name != "add_column":
            raise ComponentExecutionError(f"Unsupported reformat operation '{op_name}'.")
        name = required_str(op, "name")
        expression = op.get("expression")
        if expression is None:
            raise ComponentExecutionError("Reformat add_column requires 'expression'.")
        output = output.withColumn(name, build_expression(expression, config))

    for old_name, new_name in step.get("rename", {}).items():
        output = output.withColumnRenamed(old_name, str(new_name))

    drop_cols = step.get("drop", [])
    if drop_cols:
        output = output.drop(*drop_cols)

    if "select" in step:
        output = apply_select(output, step["select"], config)

    source = dataset(datasets, required_str(step, "input"))
    return output, source, source.limit(0)
