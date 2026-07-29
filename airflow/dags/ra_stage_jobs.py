"""STAGE namespace DAGs: source JSON/CSV data into ``risk_analytics_stage`` tables.

One DAG per source/entity pair (``ra_sourceA_customer_stage`` ...). Each DAG runs
the YAML-driven step job, so the transformation logic stays in
``transform/source_to_ods/stage_<entity>_<source>.yaml`` rather than in the DAG.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from ra_common import DEFAULT_ARGS, ENTITIES, SOURCE_LABELS, stage_dag_id, step_command

for source_label, source_key in SOURCE_LABELS.items():
    for entity in ENTITIES:
        dag_id = stage_dag_id(source_label, entity)
        with DAG(
            dag_id=dag_id,
            description=f"Load {source_label} {entity} data into the STAGE namespace via YAML transformation",
            start_date=datetime(2026, 1, 1),
            schedule=None,
            catchup=False,
            max_active_runs=1,
            default_args=DEFAULT_ARGS,
            tags=["risk-analytics", "stage", source_label, entity],
        ) as stage_dag:
            start = EmptyOperator(task_id="start")
            load_stage = BashOperator(
                task_id=f"load_{entity}_stage",
                bash_command=step_command("stage", entity, source_key),
            )
            finished = EmptyOperator(task_id="stage_load_completed")

            start >> load_stage >> finished

        globals()[dag_id] = stage_dag
