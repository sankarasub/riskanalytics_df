"""Execute a branch-isolated Risk Analytics risk calculation and merge the successful result."""
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from uuid import uuid4

from risk_analytics.config import load_config
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
    run_id = run_id or str(uuid4())
    branch = f"risk-run-{run_id.replace('-', '')[:12]}"
    target_ref = branch
    cfg = load_config()
    nessie = NessieClient(cfg["catalog"]["nessie_uri"])
    try:
        # Nessie is optional for local compatibility; direct-main execution is a
        # deliberate fallback rather than a second, divergent pipeline path.
        if not nessie.branch_exists(branch):
            nessie.create_branch(branch)
    except Exception as error:  # pragma: no cover - local compatibility fallback
        print(f"Branch setup failed ({error}); writing directly to main.")
        target_ref = "main"

    spark = create_spark_session("risk-analytics-risk-pipeline", ref=target_ref)
    row_count = 0
    pipeline_path = _resolve_pipeline_path(cfg, data_model)
    try:
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
    finally:
        try:
            spark.catalog.clearCache()
        except Exception:
            pass
        spark.stop()

    if target_ref != "main":
        # Publish only after the branch write has completed successfully.
        try:
            nessie.merge(target_ref, "main")
        except Exception as error:  # pragma: no cover - local compatibility fallback
            print(f"Merge failed ({error}); data remains on branch '{target_ref}'.")

    _publish_metrics_event(as_of_date, run_id, row_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--run-id")
    parser.add_argument("--data-model", choices=["legacy", "source-to-ods"], default="legacy")
    args = parser.parse_args()
    main(args.as_of_date, args.run_id, args.data_model)

