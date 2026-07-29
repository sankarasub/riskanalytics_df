"""Airflow DAGs for per-entity and per-source metadata transforms."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {"owner": "risk_analytics-platform", "retries": 1}
AS_OF_DATE = "{{ dag_run.conf.get('as_of_date', ds) if dag_run else ds }}"

TRANSFORMS = [
    {
        "dag_id": "risk_analytics_customer_sourcea_transform",
        "yaml": "customer_SourceA_transform.yaml",
        "extra_params": [],
    },
    {
        "dag_id": "risk_analytics_customer_sourceb_transform",
        "yaml": "customer_SourceB_transform.yaml",
        "extra_params": [
            "customer_sourceb_path={{ dag_run.conf.get('customer_sourceb_path', '/opt/risk_analytics/data/sourceb/customer/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/customer/*.csv' }}"
        ],
    },
    {
        "dag_id": "risk_analytics_asset_sourcea_transform",
        "yaml": "asset_SourceA_transform.yaml",
        "extra_params": [],
    },
    {
        "dag_id": "risk_analytics_asset_sourceb_transform",
        "yaml": "asset_SourceB_transform.yaml",
        "extra_params": [
            "asset_sourceb_path={{ dag_run.conf.get('asset_sourceb_path', '/opt/risk_analytics/data/sourceb/asset/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/asset/*.json' }}"
        ],
    },
    {
        "dag_id": "risk_analytics_product_sourcea_transform",
        "yaml": "product_SourceA_transform.yaml",
        "extra_params": [],
    },
    {
        "dag_id": "risk_analytics_product_sourceb_transform",
        "yaml": "product_SourceB_transform.yaml",
        "extra_params": [
            "product_sourceb_path={{ dag_run.conf.get('product_sourceb_path', '/opt/risk_analytics/data/sourceb/product/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/product/*.json' }}"
        ],
    },
    {
        "dag_id": "risk_analytics_trans_sourcea_transform",
        "yaml": "trans_SourceA_transform.yaml",
        "extra_params": [],
    },
    {
        "dag_id": "risk_analytics_trans_sourceb_transform",
        "yaml": "trans_SourceB_transform.yaml",
        "extra_params": [
            "trans_sourceb_path={{ dag_run.conf.get('trans_sourceb_path', '/opt/risk_analytics/data/sourceb/trans/*.csv') if dag_run else '/opt/risk_analytics/data/sourceb/trans/*.csv' }}"
        ],
    },
    {
        "dag_id": "risk_analytics_collateral_sourcea_transform",
        "yaml": "collateral_SourceA_transform.yaml",
        "extra_params": [],
    },
    {
        "dag_id": "risk_analytics_collateral_sourceb_transform",
        "yaml": "collateral_SourceB_transform.yaml",
        "extra_params": [
            "collateral_sourceb_path={{ dag_run.conf.get('collateral_sourceb_path', '/opt/risk_analytics/data/sourceb/collateral/*.json') if dag_run else '/opt/risk_analytics/data/sourceb/collateral/*.json' }}"
        ],
    },
]


def _build_bash_command(yaml_file: str, extra_params: list[str]) -> str:
    base = (
        f"env -u SPARK_REMOTE spark-submit --master local[*] "
        f"/opt/risk_analytics/scripts/tmp/execute_pipeline.py "
        f"/opt/risk_analytics/transform/{yaml_file} --param as_of_date={AS_OF_DATE}"
    )
    for param in extra_params:
        base = f"{base} --param \"{param}\""
    return base


for transform in TRANSFORMS:
    with DAG(
        dag_id=transform["dag_id"],
        description=f"Execute metadata transform {transform['yaml']}",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["risk-analytics", "transform", "metadata"],
    ) as generated_dag:
        BashOperator(
            task_id="execute_transform",
            bash_command=_build_bash_command(transform["yaml"], transform["extra_params"]),
        )

    globals()[transform["dag_id"]] = generated_dag
