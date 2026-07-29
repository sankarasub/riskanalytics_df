"""Airflow orchestration for Risk Analytics schema creation and deterministic data loading."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {"owner": "risk_analytics-platform", "retries": 1}
AS_OF_DATE = "{{ dag_run.conf.get('as_of_date', '2026-07-18') if dag_run else '2026-07-18' }}"
SOURCE_TABLES = ("customer", "asset", "collateral", "trades", "trade_product", "deals")


def bootstrap_command(action: str, table_name: str | None = None) -> str:
    command = (
        f"env -u SPARK_REMOTE spark-submit --master local[*] "
        f"/opt/risk_analytics/jobs/bootstrap.py --action {action} --as-of-date {AS_OF_DATE}"
    )
    if table_name:
        command = f"{command} --table {table_name}"
    return command


with DAG(
    dag_id="risk_analytics_create_tables_and_load_data",
    description="Create all Risk Analytics tables, seed source data, then trigger source transforms",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "iceberg", "nessie"],
) as dag:
    create_all_tables = BashOperator(
        task_id="create_all_tables",
        bash_command=bootstrap_command("create-all"),
    )

    seed_tasks = {
        "customer": BashOperator(task_id="seed_customer_table", bash_command=bootstrap_command("seed-table", "customer")),
        "asset": BashOperator(task_id="seed_asset_table", bash_command=bootstrap_command("seed-table", "asset")),
        "collateral": BashOperator(task_id="seed_collateral_table", bash_command=bootstrap_command("seed-table", "collateral")),
        "trades": BashOperator(task_id="seed_trades_table", bash_command=bootstrap_command("seed-table", "trades")),
        "trade_product": BashOperator(task_id="seed_trade_product_table", bash_command=bootstrap_command("seed-table", "trade_product")),
        "deals": BashOperator(task_id="seed_deals_table", bash_command=bootstrap_command("seed-table", "deals")),
    }

    trigger_risk_pipeline = TriggerDagRunOperator(
        task_id="trigger_source_to_ods_orchestration",
        trigger_dag_id="risk_analytics_source_to_ods_orchestration",
        conf={"as_of_date": AS_OF_DATE},
        wait_for_completion=False,
    )

    for table_name in SOURCE_TABLES:
        create_all_tables >> seed_tasks[table_name]
        seed_tasks[table_name] >> trigger_risk_pipeline

