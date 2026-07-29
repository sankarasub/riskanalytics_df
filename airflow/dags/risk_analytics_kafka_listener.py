"""Airflow DAG that listens on risk.pipeline.trigger and fires entity orchestration.

This DAG runs continuously (schedule='@continuous') with a KafkaSensor as its
first task. When a message arrives on the trigger topic — produced by
entity-stream ingest jobs or any approved producer — the sensor succeeds and
TriggerDagRunOperator kicks off kafka entity orchestration for the date carried
in the payload.

Manual pipeline runs are not affected: trigger risk_analytics_pipeline directly
and this DAG stays idle.
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

DEFAULT_ARGS = {"owner": "risk_analytics-platform", "retries": 0}

KAFKA_CONN_ID = "kafka_default"  # configure in Airflow Admin → Connections
TRIGGER_TOPIC = "risk.pipeline.trigger"


def _extract_as_of_date(message) -> dict[str, str] | None:  # noqa: ANN001
    """Validate a trigger event and return its business date through XCom.

    Returning the payload rather than a Boolean lets the following DAG receive
    the reporting date that originated in the Kafka micro-batch instead of an
    Airflow scheduler timestamp.
    """
    try:
        payload = json.loads(message.value().decode("utf-8"))
        as_of_date = payload.get("as_of_date")
        if not as_of_date:
            return None
        return {"as_of_date": str(as_of_date), "source": str(payload.get("source", "kafka"))}
    except Exception:
        return None


with DAG(
    dag_id="risk_analytics_kafka_listener",
    description="Event-driven trigger: listens on risk.pipeline.trigger and fires Kafka entity orchestration.",
    start_date=datetime(2026, 1, 1),
    schedule="@continuous",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "kafka", "event-driven"],
) as dag:

    wait_for_trigger = AwaitMessageSensor(
        task_id="wait_for_kafka_trigger",
        kafka_config_id=KAFKA_CONN_ID,
        topics=[TRIGGER_TOPIC],
        apply_function=_extract_as_of_date,
        poll_interval=5.0,
        poll_timeout=1.0,
    )

    fire_pipeline = TriggerDagRunOperator(
        task_id="trigger_source_to_ods_orchestration",
        trigger_dag_id="risk_analytics_kafka_entity_orchestration",
        conf={
            "as_of_date": "{{ ti.xcom_pull(task_ids='wait_for_kafka_trigger')['as_of_date'] }}",
            "data_model": "source-to-ods",
        },
        wait_for_completion=False,
    )

    wait_for_trigger >> fire_pipeline
