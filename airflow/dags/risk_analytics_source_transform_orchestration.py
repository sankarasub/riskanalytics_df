"""Orchestrate all source transform DAGs before final risk metrics calculation."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {"owner": "risk_analytics-platform", "retries": 1}
AS_OF_DATE = "{{ dag_run.conf.get('as_of_date', ds) if dag_run else ds }}"

TRANSFORM_DAG_IDS = [
    "risk_analytics_customer_sourcea_transform",
    "risk_analytics_customer_sourceb_transform",
    "risk_analytics_asset_sourcea_transform",
    "risk_analytics_asset_sourceb_transform",
    "risk_analytics_product_sourcea_transform",
    "risk_analytics_product_sourceb_transform",
    "risk_analytics_trans_sourcea_transform",
    "risk_analytics_trans_sourceb_transform",
    "risk_analytics_collateral_sourcea_transform",
    "risk_analytics_collateral_sourceb_transform",
]

with DAG(
    dag_id="risk_analytics_source_transform_orchestration",
    description="Run all source-specific transforms, then trigger final risk metrics DAG.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "transform", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")
    join = EmptyOperator(task_id="all_transforms_completed")

    transform_triggers = []
    for dag_id in TRANSFORM_DAG_IDS:
        task = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            conf={
                "as_of_date": AS_OF_DATE,
                "customer_sourceb_path": "{{ dag_run.conf.get('customer_sourceb_path', '/opt/risk_analytics/data/sourceb/customer/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/customer/*.csv' }}",
                "asset_sourceb_path": "{{ dag_run.conf.get('asset_sourceb_path', '/opt/risk_analytics/data/sourceb/asset/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/asset/*.json' }}",
                "product_sourceb_path": "{{ dag_run.conf.get('product_sourceb_path', '/opt/risk_analytics/data/sourceb/product/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/product/*.json' }}",
                "trans_sourceb_path": "{{ dag_run.conf.get('trans_sourceb_path', '/opt/risk_analytics/data/sourceb/trans/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/trans/*.csv' }}",
                "collateral_sourceb_path": "{{ dag_run.conf.get('collateral_sourceb_path', '/opt/risk_analytics/data/sourceb/collateral/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/collateral/*.json' }}",
            },
            wait_for_completion=True,
            poke_interval=15,
        )
        transform_triggers.append(task)
        start >> task >> join

    trigger_final_pipeline = TriggerDagRunOperator(
        task_id="trigger_final_risk_pipeline",
        trigger_dag_id="risk_analytics_pipeline",
        conf={"as_of_date": AS_OF_DATE},
        wait_for_completion=False,
    )

    join >> trigger_final_pipeline
