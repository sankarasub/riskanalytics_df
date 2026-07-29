"""Batch orchestration across the STAGE and ODS namespaces.

For every source/entity pair the matching ``ra_*_stage`` DAG runs to completion
before its ``ra_*_ods`` counterpart starts, and ``ra_riskmetrics_eval_ods`` runs
only after all ODS loads finish.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from ra_common import (
    AS_OF_DATE,
    DEFAULT_ARGS,
    ENTITIES,
    SOURCE_LABELS,
    ods_dag_id,
    sourceb_path_conf,
    stage_dag_id,
)

with DAG(
    dag_id="ra_stage_to_ods_orchestration",
    description="Run all STAGE loads, then all ODS loads, then the ODS risk metrics evaluation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["risk-analytics", "orchestration", "stage", "ods"],
) as dag:
    start = EmptyOperator(task_id="start")
    all_loads_completed = EmptyOperator(task_id="all_stage_and_ods_loads_completed")

    trigger_risk_metrics = TriggerDagRunOperator(
        task_id="trigger_ra_riskmetrics_eval_ods",
        trigger_dag_id="ra_riskmetrics_eval_ods",
        conf={"as_of_date": AS_OF_DATE, "data_model": "source-to-ods"},
        wait_for_completion=False,
    )

    for source_label in SOURCE_LABELS:
        for entity in ENTITIES:
            conf = {
                "as_of_date": AS_OF_DATE,
                "entity": entity,
                "source": source_label,
                **sourceb_path_conf(),
            }
            stage_task = TriggerDagRunOperator(
                task_id=f"trigger_{stage_dag_id(source_label, entity)}",
                trigger_dag_id=stage_dag_id(source_label, entity),
                conf=conf,
                wait_for_completion=True,
                poke_interval=15,
            )
            ods_task = TriggerDagRunOperator(
                task_id=f"trigger_{ods_dag_id(source_label, entity)}",
                trigger_dag_id=ods_dag_id(source_label, entity),
                conf=conf,
                wait_for_completion=True,
                poke_interval=15,
            )
            start.set_downstream(stage_task)
            stage_task.set_downstream(ods_task)
            ods_task.set_downstream(all_loads_completed)

    all_loads_completed >> trigger_risk_metrics
