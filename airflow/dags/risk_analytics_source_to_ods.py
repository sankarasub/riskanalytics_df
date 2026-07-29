"""Parameterized DAGs for stage and ODS loading by entity/source."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {"owner": "risk_analytics-platform", "retries": 1}
AS_OF_DATE = "{{ dag_run.conf.get('as_of_date', ds) if dag_run else ds }}"
ENTITY = "{{ dag_run.conf.get('entity', 'customer') if dag_run else 'customer' }}"
SOURCE = "{{ dag_run.conf.get('source', 'sourcea') if dag_run else 'sourcea' }}"


def _step_command(layer: str) -> str:
    base = (
        "env -u SPARK_REMOTE spark-submit --master local[*] "
        "/opt/risk_analytics/jobs/run_source_to_ods_step.py "
        f"--layer {layer} --entity {ENTITY} --source {SOURCE} --as-of-date {AS_OF_DATE} "
        "--param \"customer_sourceb_path={{ dag_run.conf.get('customer_sourceb_path', '/opt/risk_analytics/data/sourceb/customer/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/customer/*.csv' }}\" "
        "--param \"asset_sourceb_path={{ dag_run.conf.get('asset_sourceb_path', '/opt/risk_analytics/data/sourceb/asset/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/asset/*.json' }}\" "
        "--param \"product_sourceb_path={{ dag_run.conf.get('product_sourceb_path', '/opt/risk_analytics/data/sourceb/product/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/product/*.json' }}\" "
        "--param \"trans_sourceb_path={{ dag_run.conf.get('trans_sourceb_path', '/opt/risk_analytics/data/sourceb/trans/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/trans/*.csv' }}\" "
        "--param \"collateral_sourceb_path={{ dag_run.conf.get('collateral_sourceb_path', '/opt/risk_analytics/data/sourceb/collateral/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/collateral/*.json' }}\""
    )
    return base


with DAG(
    dag_id="risk_analytics_stage_load",
    description="Load source-specific data into stage namespace.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "stage", "source-to-ods"],
) as stage_dag:
    BashOperator(task_id="load_stage", bash_command=_step_command("stage"))


with DAG(
    dag_id="risk_analytics_ods_load",
    description="Merge stage data into standardized ODS namespace.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "ods", "source-to-ods"],
) as ods_dag:
    BashOperator(task_id="load_ods", bash_command=_step_command("ods"))


with DAG(
    dag_id="risk_analytics_source_to_ods_orchestration",
    description="Run stage and ODS loads for all entities and both sources, then trigger risk pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "orchestration", "source-to-ods"],
) as orchestration_dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="all_stage_and_ods_loads_completed")

    entities = ["customer", "asset", "collateral", "deals"]
    sources = ["sourcea", "sourceb"]

    for entity in entities:
        for source in sources:
            conf = {
                "as_of_date": AS_OF_DATE,
                "entity": entity,
                "source": source,
                "customer_sourceb_path": "{{ dag_run.conf.get('customer_sourceb_path', '/opt/risk_analytics/data/sourceb/customer/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/customer/*.csv' }}",
                "asset_sourceb_path": "{{ dag_run.conf.get('asset_sourceb_path', '/opt/risk_analytics/data/sourceb/asset/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/asset/*.json' }}",
                "product_sourceb_path": "{{ dag_run.conf.get('product_sourceb_path', '/opt/risk_analytics/data/sourceb/product/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/product/*.json' }}",
                "trans_sourceb_path": "{{ dag_run.conf.get('trans_sourceb_path', '/opt/risk_analytics/data/sourceb/trans/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/trans/*.csv' }}",
                "collateral_sourceb_path": "{{ dag_run.conf.get('collateral_sourceb_path', '/opt/risk_analytics/data/sourceb/collateral/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/collateral/*.json' }}",
            }
            stage_task = TriggerDagRunOperator(
                task_id=f"trigger_stage_{entity}_{source}",
                trigger_dag_id="risk_analytics_stage_load",
                conf=conf,
                wait_for_completion=True,
                poke_interval=15,
            )
            ods_task = TriggerDagRunOperator(
                task_id=f"trigger_ods_{entity}_{source}",
                trigger_dag_id="risk_analytics_ods_load",
                conf=conf,
                wait_for_completion=True,
                poke_interval=15,
            )
            start >> stage_task >> ods_task >> end

    trigger_risk_pipeline = TriggerDagRunOperator(
        task_id="trigger_risk_pipeline",
        trigger_dag_id="risk_analytics_pipeline",
        conf={"as_of_date": AS_OF_DATE, "data_model": "source-to-ods"},
        wait_for_completion=False,
    )

    end >> trigger_risk_pipeline


with DAG(
    dag_id="risk_analytics_kafka_entity_orchestration",
    description="Run stage and ODS loads for Kafka-fed SourceA entities, then trigger risk pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "orchestration", "source-to-ods", "kafka"],
) as kafka_orchestration_dag:
    start_kafka = EmptyOperator(task_id="start")
    end_kafka = EmptyOperator(task_id="all_kafka_stage_and_ods_loads_completed")

    kafka_entities = ["customer", "asset", "collateral", "deals"]
    kafka_source = "sourcea"

    for entity in kafka_entities:
        conf = {
            "as_of_date": AS_OF_DATE,
            "entity": entity,
            "source": kafka_source,
            "kafka_ingest": True,
        }
        stage_task = TriggerDagRunOperator(
            task_id=f"trigger_kafka_stage_{entity}",
            trigger_dag_id="risk_analytics_stage_load",
            conf=conf,
            wait_for_completion=True,
            poke_interval=15,
        )
        ods_task = TriggerDagRunOperator(
            task_id=f"trigger_kafka_ods_{entity}",
            trigger_dag_id="risk_analytics_ods_load",
            conf=conf,
            wait_for_completion=True,
            poke_interval=15,
        )
        start_kafka >> stage_task >> ods_task >> end_kafka

    trigger_risk_pipeline_kafka = TriggerDagRunOperator(
        task_id="trigger_risk_pipeline",
        trigger_dag_id="risk_analytics_pipeline",
        conf={"as_of_date": AS_OF_DATE, "data_model": "source-to-ods"},
        wait_for_completion=False,
    )

    end_kafka >> trigger_risk_pipeline_kafka


globals()["risk_analytics_stage_load"] = stage_dag
globals()["risk_analytics_ods_load"] = ods_dag
globals()["risk_analytics_source_to_ods_orchestration"] = orchestration_dag
globals()["risk_analytics_kafka_entity_orchestration"] = kafka_orchestration_dag
