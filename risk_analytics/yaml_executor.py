"""YAML-driven pipeline executor for Spark DataFrame transformations."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

import yaml
from pyspark.sql import DataFrame, SparkSession, functions as F

from risk_analytics.transformations import execute_component


class PipelineValidationError(ValueError):
    """Raised when a YAML pipeline definition is invalid."""


@dataclass
class PipelineExecutionResult:
    """Execution outputs and write statistics for one pipeline run."""

    datasets: dict[str, DataFrame]
    target_row_counts: dict[str, int]


def validate_pipeline_yaml(pipeline_path: str | Path, runtime_params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Validate YAML structure and template parameters without running Spark."""
    runtime_params = runtime_params or {}
    payload = _load_pipeline_yaml(Path(pipeline_path))
    rendered = _render_templates(payload, runtime_params)
    _ensure_no_unresolved_templates(rendered)
    _validate_pipeline(rendered)
    return {
        "name": rendered.get("name"),
        "source_count": len(rendered.get("sources", [])),
        "step_count": len(rendered.get("steps", [])),
        "target_count": len(rendered.get("targets", [])),
    }


def preview_pipeline_yaml(pipeline_path: str | Path, runtime_params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Render YAML templates and return resolved payload without executing Spark."""
    runtime_params = runtime_params or {}
    payload = _load_pipeline_yaml(Path(pipeline_path))
    rendered = _render_templates(payload, runtime_params)
    _ensure_no_unresolved_templates(rendered)
    _validate_pipeline(rendered)
    return {
        "summary": {
            "name": rendered.get("name"),
            "source_count": len(rendered.get("sources", [])),
            "step_count": len(rendered.get("steps", [])),
            "target_count": len(rendered.get("targets", [])),
        },
        "rendered": rendered,
    }


def run_pipeline_from_yaml(
    spark: SparkSession,
    pipeline_path: str | Path,
    config: dict[str, Any],
    runtime_params: dict[str, Any],
) -> PipelineExecutionResult:
    """Execute a declarative pipeline while retaining named intermediate datasets.

    This is the framework boundary between a human-readable YAML definition and
    Spark execution. Keeping every emitted dataset in one execution context lets
    later steps reuse prior results and makes used/unused record tracking visible
    to pipeline authors without embedding bespoke Python in each transform.
    """
    payload = _load_pipeline_yaml(Path(pipeline_path))
    payload = _render_templates(payload, runtime_params)
    _ensure_no_unresolved_templates(payload)
    _validate_pipeline(payload)

    datasets: dict[str, DataFrame] = {}
    target_row_counts: dict[str, int] = {}

    for source in payload.get("sources", []):
        name = _required_str(source, "name")
        # Source names form the initial namespace consumed by YAML steps.
        datasets[name] = _load_source(spark, source, config)

    for step in payload.get("steps", []):
        step_id = _required_str(step, "id")
        step_type = _required_str(step, "type").lower()

        # Every component returns both its business result and record-accounting
        # views, so reconciliation can be modeled consistently across operations.
        output_df, used_df, unused_df = execute_component(step_type, datasets, step, config)

        emit = step.get("emit", {})
        output_name = emit.get("output", step_id)
        datasets[output_name] = output_df

        if step.get("track_usage", True):
            used_name = emit.get("used", f"{step_id}__used")
            unused_name = emit.get("unused", f"{step_id}__unused")
            datasets[used_name] = used_df
            datasets[unused_name] = unused_df

    for target in payload.get("targets", []):
        target_name = _required_str(target, "name")
        dataset_name = _required_str(target, "dataset")
        table_name = _required_str(target, "table")
        mode = str(target.get("mode", "append")).lower()

        frame = _require_dataset(datasets, dataset_name)
        # Count before the write to return a clear, presentation-ready execution
        # summary to callers and orchestration layers.
        target_row_counts[target_name] = frame.count()

        writer = frame.writeTo(table_name)
        if mode == "append":
            writer.append()
        elif mode == "overwrite":
            writer.overwritePartitions()
        elif mode == "merge":
            _merge_target(spark, frame, table_name, target)
        else:
            raise PipelineValidationError(f"Unsupported target mode '{mode}' for target '{target_name}'.")

    _cleanup_temporary_datasets(datasets)
    return PipelineExecutionResult(datasets=datasets, target_row_counts=target_row_counts)


def _load_pipeline_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise PipelineValidationError(f"Pipeline YAML not found: {path}")
    with path.open(encoding="utf-8") as source:
        payload = yaml.safe_load(source)
    if not isinstance(payload, dict):
        raise PipelineValidationError("Pipeline YAML root must be an object.")
    return payload


def _render_templates(value: Any, runtime_params: dict[str, Any]) -> Any:
    """Recursively substitute runtime values without changing YAML value shape."""
    if isinstance(value, dict):
        return {key: _render_templates(inner, runtime_params) for key, inner in value.items()}
    if isinstance(value, list):
        return [_render_templates(item, runtime_params) for item in value]
    if isinstance(value, str):
        rendered = value
        for key, param in runtime_params.items():
            rendered = rendered.replace(f"{{{{{key}}}}}", str(param))
            rendered = rendered.replace(f"{{{{ {key} }}}}", str(param))
        return rendered
    return value


def _validate_pipeline(payload: dict[str, Any]) -> None:
    if not payload.get("sources"):
        raise PipelineValidationError("Pipeline requires non-empty 'sources'.")
    if not payload.get("steps"):
        raise PipelineValidationError("Pipeline requires non-empty 'steps'.")
    if not payload.get("targets"):
        raise PipelineValidationError("Pipeline requires non-empty 'targets'.")


def _ensure_no_unresolved_templates(payload: Any) -> None:
    """Fail early when a run would otherwise write data with literal placeholders."""
    unresolved: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for inner in value.values():
                visit(inner)
            return
        if isinstance(value, list):
            for inner in value:
                visit(inner)
            return
        if isinstance(value, str):
            unresolved.update(re.findall(r"\{\{\s*([^{}\s]+)\s*\}\}", value))

    visit(payload)
    if unresolved:
        keys = ", ".join(sorted(unresolved))
        raise PipelineValidationError(f"Unresolved runtime parameters in pipeline templates: {keys}")


def _load_source(spark: SparkSession, source_spec: dict[str, Any], config: dict[str, Any]) -> DataFrame:
    """Materialize a table, SQL, configuration map, or external-file source.

    Source handling is centralized so all pipeline definitions receive the same
    validation and optional selection/filter behavior regardless of input type.
    """
    source_type = str(source_spec.get("type", "table")).lower()

    if source_type == "table":
        table_name = _required_str(source_spec, "table")
        output = spark.table(table_name)
        if "select" in source_spec:
            output = output.select(*source_spec["select"])
        for condition in source_spec.get("filters", []):
            output = output.filter(F.expr(str(condition)))
        return output

    if source_type == "sql":
        query = _required_str(source_spec, "query")
        return spark.sql(query)

    if source_type == "config_map":
        config_path = _required_str(source_spec, "config_path")
        key_column = str(source_spec.get("key_column", "key"))
        value_column = str(source_spec.get("value_column", "value"))
        mapping = _config_value(config, config_path)
        if not isinstance(mapping, dict):
            raise PipelineValidationError(f"Source config_map path '{config_path}' must resolve to an object.")
        rows = [{key_column: key, value_column: value} for key, value in mapping.items()]
        return spark.createDataFrame(rows)

    if source_type in {"file", "files"}:
        fmt = str(source_spec.get("format", "parquet")).lower()
        path = _required_str(source_spec, "path")
        options = source_spec.get("options", {})
        if options is None:
            options = {}
        if not isinstance(options, dict):
            raise PipelineValidationError("Source 'options' must be an object when type=file.")

        reader = spark.read.format(fmt)
        for key, value in options.items():
            reader = reader.option(str(key), str(value))
        output = reader.load(path)

        if "select" in source_spec:
            output = output.select(*source_spec["select"])
        for condition in source_spec.get("filters", []):
            output = output.filter(F.expr(str(condition)))
        return output

    raise PipelineValidationError(f"Unsupported source type '{source_type}'.")


def _merge_target(spark: SparkSession, frame: DataFrame, table_name: str, target_spec: dict[str, Any]) -> None:
    """Upsert a DataFrame into an Iceberg target and always remove its temp view.

    Some local Spark/catalog combinations cannot plan SQL ``MERGE`` against a
    temporary view. The targeted fallback retains usable local execution while
    allowing full merge semantics where the catalog supports them.
    """
    keys = target_spec.get("keys")
    if not isinstance(keys, list) or not keys or not all(isinstance(item, str) and item.strip() for item in keys):
        raise PipelineValidationError(f"Merge mode for target '{target_spec.get('name', table_name)}' requires non-empty list field 'keys'.")

    key_names = [item.strip() for item in keys]
    source_columns = frame.columns
    if not source_columns:
        raise PipelineValidationError(f"Target '{target_spec.get('name', table_name)}' cannot merge an empty schema.")

    merge_condition = " AND ".join([f"target.{name} = source.{name}" for name in key_names])
    update_columns = [name for name in source_columns if name not in key_names]
    if update_columns:
        update_set = ", ".join([f"target.{name} = source.{name}" for name in update_columns])
    else:
        update_set = ", ".join([f"target.{name} = source.{name}" for name in key_names])

    insert_columns = ", ".join(source_columns)
    insert_values = ", ".join([f"source.{name}" for name in source_columns])
    temp_view = f"__yaml_merge_{uuid4().hex}"

    # A unique name prevents concurrent pipeline runs from colliding in Spark's
    # shared temporary-view namespace.
    frame.createOrReplaceTempView(temp_view)
    try:
        try:
            spark.sql(
                f"""
                MERGE INTO {table_name} AS target
                USING {temp_view} AS source
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
                """
            )
        except Exception as error:
            if "No plan for TableReference" not in str(error):
                raise
            frame.writeTo(table_name).overwritePartitions()
    finally:
        spark.catalog.dropTempView(temp_view)


def _cleanup_temporary_datasets(datasets: dict[str, DataFrame]) -> None:
    """Release cached reconciliation frames without disturbing named outputs."""
    for name, frame in datasets.items():
        if not isinstance(frame, DataFrame):
            continue
        try:
            if name.endswith("__used") or name.endswith("__unused"):
                frame.unpersist(blocking=False)
        except Exception:
            continue


def _required_str(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise PipelineValidationError(f"Field '{key}' must be a non-empty string.")
    return value.strip()


def _require_dataset(datasets: dict[str, DataFrame], name: str) -> DataFrame:
    try:
        return datasets[name]
    except KeyError as error:
        raise PipelineValidationError(f"Dataset '{name}' not found in executor context.") from error


def _config_value(config: dict[str, Any], path: str) -> Any:
    current: Any = config
    for chunk in path.split("."):
        if not isinstance(current, dict) or chunk not in current:
            raise PipelineValidationError(f"Config path '{path}' could not be resolved.")
        current = current[chunk]
    return current
