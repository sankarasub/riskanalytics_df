"""Event-driven Kafka DAGs for streaming customer updates.

``ra_kafka_customer_stage`` waits for a micro-batch notification published by the
Spark structured-streaming consumer (``jobs/kafka_entity_consumer.py``), loads the
customer STAGE table for that business date, and hands over to
``ra_kafka_customer_ods``. Both DAGs trigger ``ra_riskmetrics_eval_ods`` directly so
streaming updates are reflected in the published metrics; the ODS DAG skips its own
trigger when the STAGE DAG already owns it, which keeps one metrics run per batch.
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

from ra_common import AS_OF_DATE, DEFAULT_ARGS, step_command

KAFKA_CONN_ID = "kafka_default"  # configured by airflow-init in docker-compose
TRIGGER_TOPIC = "risk.pipeline.trigger"
KAFKA_SOURCE = "sourcea"
STREAMED_AS_OF_DATE = "{{ ti.xcom_pull(task_ids='wait_for_customer_event')['as_of_date'] }}"


def extract_as_of_date(message) -> dict[str, str] | None:  # noqa: ANN001 - Kafka message type
    """Return the business date carried by a micro-batch event through XCom.

    Returning the payload rather than a Boolean lets downstream tasks use the
    reporting date that originated in the Kafka batch instead of an Airflow
    scheduler timestamp.
    """
    try:
        payload = json.loads(message.value().decode("utf-8"))
    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    as_of_date = payload.get("as_of_date")
    if not as_of_date:
        return None
    return {"as_of_date": str(as_of_date), "source": str(payload.get("source", "kafka"))}


def should_trigger_risk_metrics(**context) -> bool:
    """Skip the metrics trigger when the calling DAG already owns it."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    return bool(conf.get("trigger_riskmetrics", True))


with DAG(
    dag_id="ra_kafka_customer_stage",
    description="Event-driven customer STAGE load for Kafka micro-batches",
    start_date=datetime(2026, 1, 1),
    schedule="@continuous",
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["risk-analytics", "kafka", "stage", "event-driven"],
) as kafka_stage_dag:
    wait_for_customer_event = AwaitMessageSensor(
        task_id="wait_for_customer_event",
        kafka_config_id=KAFKA_CONN_ID,
        topics=[TRIGGER_TOPIC],
        apply_function=extract_as_of_date,
        poll_interval=5.0,
        poll_timeout=1.0,
    )

    load_customer_stage = BashOperator(
        task_id="load_customer_stage",
        bash_command=step_command("stage", "customer", KAFKA_SOURCE, as_of_date=STREAMED_AS_OF_DATE),
    )

    trigger_kafka_customer_ods = TriggerDagRunOperator(
        task_id="trigger_ra_kafka_customer_ods",
        trigger_dag_id="ra_kafka_customer_ods",
        conf={"as_of_date": STREAMED_AS_OF_DATE, "trigger_riskmetrics": False},
        wait_for_completion=True,
        poke_interval=15,
    )

    trigger_riskmetrics_after_stream = TriggerDagRunOperator(
        task_id="trigger_ra_riskmetrics_eval_ods",
        trigger_dag_id="ra_riskmetrics_eval_ods",
        conf={"as_of_date": STREAMED_AS_OF_DATE, "data_model": "source-to-ods"},
        wait_for_completion=False,
    )

    wait_for_customer_event >> load_customer_stage >> trigger_kafka_customer_ods >> trigger_riskmetrics_after_stream


with DAG(
    dag_id="ra_kafka_customer_ods",
    description="Event-driven customer ODS standardization for Kafka micro-batches",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["risk-analytics", "kafka", "ods", "event-driven"],
) as kafka_ods_dag:
    load_customer_ods = BashOperator(
        task_id="load_customer_ods",
        bash_command=step_command("ods", "customer", KAFKA_SOURCE, as_of_date=AS_OF_DATE),
    )

    check_metrics_ownership = ShortCircuitOperator(
        task_id="check_riskmetrics_trigger_requested",
        python_callable=should_trigger_risk_metrics,
    )

    trigger_riskmetrics_after_ods = TriggerDagRunOperator(
        task_id="trigger_ra_riskmetrics_eval_ods",
        trigger_dag_id="ra_riskmetrics_eval_ods",
        conf={"as_of_date": AS_OF_DATE, "data_model": "source-to-ods"},
        wait_for_completion=False,
    )

    load_customer_ods >> check_metrics_ownership >> trigger_riskmetrics_after_ods


globals()["ra_kafka_customer_stage"] = kafka_stage_dag
globals()["ra_kafka_customer_ods"] = kafka_ods_dag
