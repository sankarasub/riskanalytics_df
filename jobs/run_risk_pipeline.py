"""Execute a branch-isolated Risk Analytics risk calculation and merge the successful result."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from uuid import uuid4

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from risk_analytics.config import load_config
from risk_analytics.logging_config import PipelineLogger, setup_logging
from risk_analytics.nessie import NessieClient
from risk_analytics.spark import create_spark_session
from risk_analytics.yaml_executor import run_pipeline_from_yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_MODEL_PIPELINES = {
    "legacy": "transform/risk_metrics_pipeline.yaml",
    "source-to-ods": str((REPO_ROOT / "transform" / "source_to_ods" / "risk_metrics_pipeline_source_to_ods.yaml").resolve()),
}


def _publish_metrics_event(as_of_date: str, run_id: str, row_count: int) -> None:
    """Publish a non-blocking completion event when Kafka is configured.

    Publishing is intentionally best-effort: lakehouse output is the system of
    record, so an optional notification outage must not invalidate a successful
    calculation or force the platform to roll back published data.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    if not bootstrap:
        return
    try:
        from confluent_kafka import Producer  # lazy import — optional dependency

        producer = Producer({"bootstrap.servers": bootstrap})
        payload = json.dumps(
            {"as_of_date": as_of_date, "run_id": run_id, "row_count": row_count}
        ).encode()
        producer.produce("risk.metrics.published", value=payload)
        producer.flush(timeout=10)
        print(f"[run_risk_pipeline] published metrics event for {as_of_date}")
    except Exception as exc:  # pragma: no cover
        print(f"[run_risk_pipeline] Kafka publish skipped ({exc})")


def _resolve_pipeline_path(cfg: dict, data_model: str) -> str:
    if data_model not in DATA_MODEL_PIPELINES:
        raise ValueError(f"Unsupported data model '{data_model}'.")
    # Explicit override supports controlled operational fallbacks.
    if data_model == "legacy":
        return cfg["executor"]["pipeline_path"]
    return os.getenv("RISK_PIPELINE_YAML_SOURCE_TO_ODS", DATA_MODEL_PIPELINES[data_model])


def main(as_of_date: str, run_id: str | None = None, data_model: str = "legacy") -> None:
    """Run the final risk YAML pipeline in an isolated Nessie reference.

    A branch is created before Spark writes begin and merged only after Spark has
    stopped cleanly. This separates in-flight calculation results from the
    published reference and gives each output a reproducible run identifier.
    """
    # Setup logging
    setup_logging()
    logger = PipelineLogger("risk_metrics_pipeline")
    
    run_id = run_id or str(uuid4())
    branch = f"risk-run-{run_id.replace('-', '')[:12]}"
    target_ref = branch
    cfg = load_config()
    execution_mode = cfg.get("execution_mode", "docker")
    
    params = {
        "as_of_date": as_of_date,
        "run_id": run_id,
        "data_model": data_model,
        "execution_mode": execution_mode,
        "branch": branch,
        "target_ref": target_ref
    }
    
    logger.log_pipeline_start(params)
    
    nessie = NessieClient(cfg["catalog"]["nessie_uri"])
    try:
        # Nessie is optional for local compatibility; direct-main execution is a
        # deliberate fallback rather than a second, divergent pipeline path.
        logger.log_step_start("create_nessie_branch", {"branch": branch})
        if not nessie.branch_exists(branch):
            nessie.create_branch(branch)
        logger.log_step_complete("create_nessie_branch")
    except Exception as error:  # pragma: no cover - local compatibility fallback
        logger.log_step_error("create_nessie_branch", error)
        logger.log_step_start("direct_main_fallback", {"reason": str(error)})
        target_ref = "main"
        logger.log_step_complete("direct_main_fallback")

    spark = create_spark_session("risk-analytics-risk-pipeline", ref=target_ref)
    logger.log_spark_operation("session_created", {"app_name": "risk-analytics-risk-pipeline", "ref": target_ref})
    
    row_count = 0
    pipeline_path = _resolve_pipeline_path(cfg, data_model)
    
    try:
        logger.log_step_start("run_risk_pipeline", {"data_model": data_model, "pipeline_path": pipeline_path})
        start_time = time.time()
        
        # Runtime values resolve the YAML templates and are persisted in output
        # columns, linking each metric row to this exact execution context.
        execution = run_pipeline_from_yaml(
            spark=spark,
            pipeline_path=pipeline_path,
            config=cfg,
            runtime_params={
                "as_of_date": as_of_date,
                "risk_run_id": run_id,
                "source_branch": target_ref,
                "data_model": data_model,
            },
        )
        row_count = execution.target_row_counts.get("risk_metrics", 0)
        duration = (time.time() - start_time) * 1000
        
        logger.log_step_complete("run_risk_pipeline", row_count, duration)
        logger.log_table_operation("write", "risk_metrics", {"records": row_count})
        
    finally:
        try:
            spark.catalog.clearCache()
        except Exception:
            pass
        spark.stop()
        logger.log_spark_operation("session_stopped")

    if target_ref != "main":
        # Publish only after the branch write has completed successfully.
        try:
            logger.log_step_start("merge_nessie_branch", {"from": target_ref, "to": "main"})
            nessie.merge(target_ref, "main")
            logger.log_step_complete("merge_nessie_branch")
        except Exception as error:  # pragma: no cover - local compatibility fallback
            logger.log_step_error("merge_nessie_branch", error)
            print(f"Merge failed ({error}); data remains on branch '{target_ref}'.")

    logger.log_step_start("publish_metrics_event", {"as_of_date": as_of_date, "run_id": run_id, "row_count": row_count})
    _publish_metrics_event(as_of_date, run_id, row_count)
    logger.log_step_complete("publish_metrics_event")
    
    logger.log_pipeline_complete(success=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--run-id")
    parser.add_argument("--data-model", choices=["legacy", "source-to-ods"], default="legacy")
    args = parser.parse_args()
    main(args.as_of_date, args.run_id, args.data_model)

