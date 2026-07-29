"""Final risk metrics DAG: evaluate risk metrics from the ODS namespace.

Downstream of every ``ra_*_ods`` load. Also triggered directly by the Kafka
streaming DAGs so metrics follow streaming micro-batches.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from ra_common import AS_OF_DATE, DEFAULT_ARGS, SPARK_POOL, risk_metrics_command

DATA_MODEL = "{{ dag_run.conf.get('data_model', 'source-to-ods') if dag_run else 'source-to-ods' }}"

with DAG(
    dag_id="ra_riskmetrics_eval_ods",
    description="Branch-safe risk metrics evaluation (PFE, VaR, exposure) from ODS tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "ods", "risk-metrics"],
) as dag:
    evaluate_risk_metrics = BashOperator(
        task_id="evaluate_and_publish_risk_metrics",
        bash_command=risk_metrics_command(AS_OF_DATE, DATA_MODEL),
        pool=SPARK_POOL,
    )
