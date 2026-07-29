"""Event-driven Kafka DAGs, one STAGE/ODS pair per entity.

``ra_kafka_<entity>_stage`` waits for a micro-batch notification published by the
Spark structured-streaming consumer (``jobs/kafka_entity_consumer.py``), loads that
entity's STAGE table for the business date carried by the event, and hands over to
``ra_kafka_<entity>_ods``. The STAGE DAG owns the ``ra_riskmetrics_eval_ods`` trigger
and tells the ODS DAG to skip its own, which keeps one metrics run per micro-batch
while leaving the ODS DAG independently runnable.

The sensors defer, so the ``airflow-triggerer`` service must be running.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

from ra_common import (
    AS_OF_DATE,
    DEFAULT_ARGS,
    ENTITIES,
    KAFKA_CONN_ID,
    KAFKA_SOURCE,
    SPARK_POOL,
    kafka_ods_dag_id,
    kafka_stage_dag_id,
    step_command,
)
from risk_analytics.kafka_events import TRIGGER_TOPIC

# Resolved with ``import_string`` inside the triggerer, so it must be a dotted path.
MATCH_FUNCTION = "risk_analytics.kafka_events.match_entity_event"


def should_trigger_risk_metrics(**context) -> bool:
    """Skip the metrics trigger when the calling DAG already owns it."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    return bool(conf.get("trigger_riskmetrics", True))


def _streamed_as_of_date(sensor_task_id: str) -> str:
    return f"{{{{ ti.xcom_pull(task_ids='{sensor_task_id}')['as_of_date'] }}}}"


for entity in ENTITIES:
    stage_dag_id = kafka_stage_dag_id(entity)
    ods_dag_id = kafka_ods_dag_id(entity)
    sensor_task_id = f"wait_for_{entity}_event"
    streamed_as_of_date = _streamed_as_of_date(sensor_task_id)

    with DAG(
        dag_id=stage_dag_id,
        description=f"Event-driven {entity} STAGE load for Kafka micro-batches",
        start_date=datetime(2026, 1, 1),
        schedule="@continuous",
        catchup=False,
        max_active_runs=1,
        default_args={**DEFAULT_ARGS, "retries": 0},
        tags=["risk-analytics", "kafka", "stage", "event-driven", entity],
    ) as kafka_stage_dag:
        wait_for_event = AwaitMessageSensor(
            task_id=sensor_task_id,
            kafka_config_id=KAFKA_CONN_ID,
            topics=[TRIGGER_TOPIC],
            apply_function=MATCH_FUNCTION,
            apply_function_kwargs={"entity": entity},
            poll_interval=5.0,
            poll_timeout=1.0,
        )

        load_stage = BashOperator(
            task_id=f"load_{entity}_stage",
            bash_command=step_command("stage", entity, KAFKA_SOURCE, as_of_date=streamed_as_of_date),
            pool=SPARK_POOL,
        )

        trigger_kafka_ods = TriggerDagRunOperator(
            task_id=f"trigger_{ods_dag_id}",
            trigger_dag_id=ods_dag_id,
            conf={"as_of_date": streamed_as_of_date, "trigger_riskmetrics": False},
            wait_for_completion=True,
            deferrable=True,
            poke_interval=15,
        )

        trigger_riskmetrics_after_stream = TriggerDagRunOperator(
            task_id="trigger_ra_riskmetrics_eval_ods",
            trigger_dag_id="ra_riskmetrics_eval_ods",
            conf={"as_of_date": streamed_as_of_date, "data_model": "source-to-ods"},
            wait_for_completion=False,
        )

        wait_for_event >> load_stage >> trigger_kafka_ods >> trigger_riskmetrics_after_stream

    globals()[stage_dag_id] = kafka_stage_dag

    with DAG(
        dag_id=ods_dag_id,
        description=f"Event-driven {entity} ODS standardization for Kafka micro-batches",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        default_args={**DEFAULT_ARGS, "retries": 0},
        tags=["risk-analytics", "kafka", "ods", "event-driven", entity],
    ) as kafka_ods_dag:
        load_ods = BashOperator(
            task_id=f"load_{entity}_ods",
            bash_command=step_command("ods", entity, KAFKA_SOURCE, as_of_date=AS_OF_DATE),
            pool=SPARK_POOL,
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

        load_ods >> check_metrics_ownership >> trigger_riskmetrics_after_ods

    globals()[ods_dag_id] = kafka_ods_dag
