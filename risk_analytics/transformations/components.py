"""Composable DataFrame transformation dispatcher for YAML-driven risk pipelines."""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame

from risk_analytics.transformations.aggregation import run_dedup, run_normalize, run_rollup
from risk_analytics.transformations.common import ComponentExecutionError
from risk_analytics.transformations.relational import run_join, run_lookup
from risk_analytics.transformations.shaping import run_filter, run_reformat


def execute_component(component: str, datasets: dict[str, DataFrame], step: dict[str, Any], config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Dispatch one approved YAML component through a uniform result contract.

    The registry is an intentional extension point: adding a transformation is a
    bounded change to a dedicated handler, rather than a new special case in the
    executor. Every handler returns output, used, and unused DataFrames so the
    framework can expose consistent reconciliation semantics.
    """
    handlers = {
        "join": run_join,
        "lookup": run_lookup,
        "rollup": run_rollup,
        "normalize": run_normalize,
        "dedup": run_dedup,
        "filter": run_filter,
        "reformat": run_reformat,
    }
    try:
        handler = handlers[component]
    except KeyError as error:
        raise ComponentExecutionError(f"Unsupported component '{component}'.") from error
    return handler(datasets, step, config)
