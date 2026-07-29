"""Shared helpers and exceptions for transformation components."""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame


class ComponentExecutionError(ValueError):
    """Raised when a transformation component specification is invalid."""


def dataset(datasets: dict[str, DataFrame], name: str) -> DataFrame:
    try:
        return datasets[name]
    except KeyError as error:
        raise ComponentExecutionError(f"Dataset '{name}' was not found in execution context.") from error


def required_str(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ComponentExecutionError(f"Field '{key}' must be a non-empty string.")
    return value.strip()


def config_value(config: dict[str, Any], path: str) -> Any:
    current: Any = config
    for chunk in path.split("."):
        if not isinstance(current, dict) or chunk not in current:
            raise ComponentExecutionError(f"Config path '{path}' could not be resolved.")
        current = current[chunk]
    return current
