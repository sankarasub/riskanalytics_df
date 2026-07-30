"""Run source-to-ODS layer steps for the selected entities and source.

``--entity`` may be repeated so several entities share one Spark session; each JVM
start costs more than the transformation itself for these table sizes.
"""
from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

from risk_analytics.config import load_config
from risk_analytics.logging_config import PipelineLogger, setup_logging
from risk_analytics.spark import create_spark_session
from risk_analytics.yaml_executor import run_pipeline_from_yaml

SUPPORTED_LAYERS = {"stage", "ods"}
SUPPORTED_ENTITIES = {"customer", "asset", "collateral", "deals"}
SUPPORTED_SOURCES = {"sourcea", "sourceb"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=sorted(SUPPORTED_LAYERS), required=True)
    parser.add_argument(
        "--entity",
        action="append",
        choices=sorted(SUPPORTED_ENTITIES),
        required=True,
        help="Entity to load; repeat to run several entities in one Spark session.",
    )
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


def _pipeline_path(layer: str, entity: str, source: str) -> Path:
    path = (
        Path(__file__).resolve().parent.parent
        / "transform"
        / "source_to_ods"
        / f"{layer}_{entity}_{source}.yaml"
    )
    if not path.exists():
        raise FileNotFoundError(f"Pipeline YAML not found for step: {path}")
    return path


def main() -> None:
    args = parse_args()
    entities = list(dict.fromkeys(args.entity))
    overrides = _parse_runtime_params(args.param)
    pipeline_paths = {entity: _pipeline_path(args.layer, entity, args.source) for entity in entities}

    # Setup logging
    setup_logging()
    logger = PipelineLogger(f"source_to_ods_{args.layer}_{args.source}")
    
    config = load_config()
    execution_mode = config.get("execution_mode", "docker")
    
    params = {
        "layer": args.layer,
        "source": args.source,
        "entities": entities,
        "as_of_date": args.as_of_date,
        "execution_mode": execution_mode,
        "overrides": overrides
    }
    
    logger.log_pipeline_start(params)
    
    # Set default file paths for local/hybrid modes
    repo_root = Path(__file__).resolve().parent.parent
    data_dir = repo_root / "data" / "sourcea"
    
    if execution_mode in ["local", "hybrid"]:
        # Add file paths for SourceA
        if args.source == "sourcea":
            overrides.setdefault("customer_sourcea_path", str(data_dir / "customer.json"))
            overrides.setdefault("asset_sourcea_path", str(data_dir / "asset.json"))
            overrides.setdefault("collateral_sourcea_path", str(data_dir / "collateral.json"))
            overrides.setdefault("deals_sourcea_path", str(data_dir / "deals.json"))
            logger.log_step_start("configure_file_paths", {"source": "sourcea", "mode": execution_mode})
        # Add file paths for SourceB
        elif args.source == "sourceb":
            data_dir_b = repo_root / "data" / "sourceb"
            overrides.setdefault("customer_sourceb_path", str(data_dir_b / "customer" / "*.csv"))
            overrides.setdefault("asset_sourceb_path", str(data_dir_b / "asset" / "asset_sourceb_sample.json"))
            overrides.setdefault("collateral_sourceb_path", str(data_dir_b / "collateral" / "collateral_sourceb_sample.json"))
            overrides.setdefault("deals_sourceb_path", str(data_dir_b / "trans" / "*.csv"))
            logger.log_step_start("configure_file_paths", {"source": "sourceb", "mode": execution_mode})
        logger.log_step_complete("configure_file_paths")
    
    spark = create_spark_session(args.app_name, ref=args.spark_ref)
    logger.log_spark_operation("session_created", {"app_name": args.app_name, "ref": args.spark_ref})
    
    try:
        for entity in entities:
            step_name = f"{args.layer}_{entity}_{args.source}"
            logger.log_step_start(step_name, {"entity": entity, "layer": args.layer, "source": args.source})
            
            start_time = time.time()
            run_pipeline_from_yaml(
                spark=spark,
                pipeline_path=pipeline_paths[entity],
                config=config,
                runtime_params={
                    "as_of_date": args.as_of_date,
                    "source": args.source,
                    "entity": entity,
                    "layer": args.layer,
                    **overrides,
                },
            )
            duration = (time.time() - start_time) * 1000
            
            # Get record count if possible
            try:
                if args.layer == "stage":
                    table_name = f"nessie.risk_analytics_stage.{entity}_stage_{args.source}"
                else:
                    table_name = f"nessie.risk_analytics_ods.{entity}"
                
                records = spark.table(table_name).count()
                logger.log_table_operation("write", table_name, {"records": records})
                logger.log_step_complete(step_name, records, duration)
            except Exception as e:
                logger.log_step_complete(step_name, 0, duration)
                logger.log_table_operation("write", f"{args.layer}_{entity}", {"error": str(e)})
        
        logger.log_pipeline_complete(success=True)
        
    except Exception as e:
        logger.log_step_error(f"{args.layer}_pipeline", e)
        logger.log_pipeline_complete(success=False)
        raise
    finally:
        spark.stop()
        logger.log_spark_operation("session_stopped")


if __name__ == "__main__":
    main()
