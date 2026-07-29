"""ODS namespace DAGs: standardize STAGE data into ``risk_analytics_ods`` tables.

One DAG per source/entity pair (``ra_sourceA_customer_ods`` ...). The schema
standardization rules live in ``transform/source_to_ods/ods_<entity>_<source>.yaml``.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from ra_common import DEFAULT_ARGS, ENTITIES, SOURCE_LABELS, ods_dag_id, step_command

for source_label, source_key in SOURCE_LABELS.items():
    for entity in ENTITIES:
        dag_id = ods_dag_id(source_label, entity)
        with DAG(
            dag_id=dag_id,
            description=f"Standardize {source_label} {entity} STAGE data into the ODS namespace via YAML transformation",
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
            max_active_runs=1,
            default_args=DEFAULT_ARGS,
            tags=["risk-analytics", "ods", source_label, entity],
        ) as ods_dag:
            start = EmptyOperator(task_id="start")
            load_ods = BashOperator(
                task_id=f"load_{entity}_ods",
                bash_command=step_command("ods", entity, source_key),
            )
            finished = EmptyOperator(task_id="ods_load_completed")

            start >> load_ods >> finished

        globals()[dag_id] = ods_dag
