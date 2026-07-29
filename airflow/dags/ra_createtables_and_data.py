"""Bootstrap DAG: create every Risk Analytics table, then seed source data."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from ra_common import AS_OF_DATE, DEFAULT_ARGS, SPARK_POOL, bootstrap_command

SOURCE_TABLES = ("customer", "asset", "collateral", "trades", "trade_product", "deals")

with DAG(
    dag_id="ra_createtables_and_data",
    description="Create all Risk Analytics tables, seed source data, then trigger the STAGE/ODS orchestration",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "bootstrap", "iceberg", "nessie"],
) as dag:
    create_all_tables = BashOperator(
        task_id="create_all_tables",
        bash_command=bootstrap_command("create-all"),
        pool=SPARK_POOL,
    )

    seed_completed = EmptyOperator(task_id="all_source_tables_seeded")

    trigger_stage_to_ods = TriggerDagRunOperator(
        task_id="trigger_ra_stage_to_ods_orchestration",
        trigger_dag_id="ra_stage_to_ods_orchestration",
        conf={"as_of_date": AS_OF_DATE},
        wait_for_completion=False,
    )

    for table_name in SOURCE_TABLES:
        seed_task = BashOperator(
            task_id=f"seed_{table_name}_table",
            bash_command=bootstrap_command("seed-table", table_name=table_name),
            pool=SPARK_POOL,
        )
        create_all_tables >> seed_task >> seed_completed

    seed_completed >> trigger_stage_to_ods
