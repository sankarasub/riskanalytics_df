"""Airflow orchestration only; risk mathematics remains in the Spark package."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {"owner": "risk_analytics-platform", "retries": 1}
AS_OF_DATE = "{{ dag_run.conf.get('as_of_date', ds) if dag_run else ds }}"
DATA_MODEL = "{{ dag_run.conf.get('data_model', 'legacy') if dag_run else 'legacy' }}"

with DAG(
    dag_id="risk_analytics_pipeline",
    description="Final branch-safe risk metrics aggregation after source transforms",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "iceberg", "nessie"],
) as dag:
    calculate = BashOperator(
        task_id="calculate_and_publish_risk",
        bash_command=f"env -u SPARK_REMOTE spark-submit --master local[*] /opt/risk_analytics/jobs/run_risk_pipeline.py --as-of-date {AS_OF_DATE} --run-id {{{{ run_id }}}} --data-model {DATA_MODEL}",
    )

