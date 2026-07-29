"""Generic CLI entrypoint to execute any metadata YAML pipeline."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from risk_analytics.config import load_config
from risk_analytics.spark import create_spark_session
from risk_analytics.yaml_executor import PipelineValidationError, run_pipeline_from_yaml


def _parse_pairs(values: list[str], argument_name: str) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"{argument_name} entries must be KEY=VALUE, got '{raw}'.")
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"{argument_name} key cannot be empty for '{raw}'.")
        output[key] = _coerce(value.strip())
    return output


def _coerce(value: str) -> Any:
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if (value.startswith("{") and value.endswith("}")) or (value.startswith("[") and value.endswith("]")):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute a YAML transformation pipeline")
    parser.add_argument("pipeline", help="Pipeline YAML path (absolute or repo-relative)")
    parser.add_argument("--param", action="append", default=[], help="Runtime parameter KEY=VALUE")
    parser.add_argument(
        "--required-parameters",
        dest="required_parameters",
        action="append",
        default=[],
        help="Alias for --param. Provide KEY=VALUE runtime parameters.",
    )
    parser.add_argument("--config", action="append", default=[], help="Config override PATH.TO.KEY=VALUE")
    parser.add_argument("--spark-ref", default="main", help="Nessie branch reference for Spark catalog")
    parser.add_argument("--app-name", default="risk-analytics-yaml-executor", help="Spark app name")
    return parser.parse_args()


def _apply_config_overrides(config: dict[str, Any], overrides: dict[str, Any]) -> None:
    for path, value in overrides.items():
        current = config
        chunks = [chunk.strip() for chunk in path.split(".") if chunk.strip()]
        if not chunks:
            raise ValueError(f"Invalid config override path '{path}'.")
        for chunk in chunks[:-1]:
            current = current.setdefault(chunk, {})
            if not isinstance(current, dict):
                raise ValueError(f"Config override path '{path}' traverses a non-object segment.")
        current[chunks[-1]] = value


def main() -> int:
    args = parse_args()
    cfg = load_config()

    runtime_params = _parse_pairs(args.param, "--param")
    runtime_params.update(_parse_pairs(args.required_parameters, "--required-parameters"))
    config_overrides = _parse_pairs(args.config, "--config")
    _apply_config_overrides(cfg, config_overrides)

    pipeline_path = Path(args.pipeline)
    if not pipeline_path.is_absolute():
        pipeline_path = Path(__file__).resolve().parent / pipeline_path

    spark = create_spark_session(args.app_name, ref=args.spark_ref)
    try:
        result = run_pipeline_from_yaml(
            spark=spark,
            pipeline_path=pipeline_path,
            config=cfg,
            runtime_params=runtime_params,
        )
    except PipelineValidationError as error:
        print(f"Pipeline validation failed: {error}")
        return 2
    finally:
        spark.stop()

    print(f"Pipeline '{pipeline_path}' completed successfully.")
    for target_name, row_count in result.target_row_counts.items():
        print(f"  - {target_name}: {row_count} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
