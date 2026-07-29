"""Run one source-to-ODS layer step for a selected entity and source."""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from risk_analytics.config import load_config
from risk_analytics.spark import create_spark_session
from risk_analytics.yaml_executor import run_pipeline_from_yaml


SUPPORTED_LAYERS = {"stage", "ods"}
SUPPORTED_ENTITIES = {"customer", "asset", "collateral", "deals"}
SUPPORTED_SOURCES = {"sourcea", "sourceb"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=sorted(SUPPORTED_LAYERS), required=True)
    parser.add_argument("--entity", choices=sorted(SUPPORTED_ENTITIES), required=True)
    parser.add_argument("--source", choices=sorted(SUPPORTED_SOURCES), required=True)
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--spark-ref", default="main")
    parser.add_argument("--app-name", default="risk-analytics-source-to-ods")
    parser.add_argument("--param", action="append", default=[])
    return parser.parse_args()


def _parse_runtime_params(values: list[str]) -> dict[str, str]:
    output: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"Invalid --param '{item}'. Expected key=value.")
        key, value = item.split("=", 1)
        output[key.strip()] = value.strip()
    return output


def main() -> None:
    args = parse_args()
    runtime_params = {
        "as_of_date": args.as_of_date,
        "source": args.source,
        "entity": args.entity,
        "layer": args.layer,
        **_parse_runtime_params(args.param),
    }

    pipeline_path = (
        Path(__file__).resolve().parent.parent
        / "transform"
        / "source_to_ods"
        / f"{args.layer}_{args.entity}_{args.source}.yaml"
    )
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Pipeline YAML not found for step: {pipeline_path}")

    config = load_config()
    spark = create_spark_session(args.app_name, ref=args.spark_ref)
    try:
        run_pipeline_from_yaml(
            spark=spark,
            pipeline_path=pipeline_path,
            config=config,
            runtime_params=runtime_params,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
