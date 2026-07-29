"""Batch orchestration across the STAGE and ODS namespaces.

Each source/entity pair is a task group that runs its ``ra_*_stage`` DAG to
completion before the matching ``ra_*_ods`` DAG starts; the eight groups run
concurrently and ``ra_riskmetrics_eval_ods`` runs once every ODS load finishes.

The triggers wait in deferred state (``deferrable=True``), so they hold a triggerer
slot instead of an executor slot while a child DAG runs; the actual spark-submit
concurrency is bounded by the ``spark_submit`` pool the leaf DAGs use.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

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
            with TaskGroup(group_id=f"{source_label}_{entity}") as entity_group:
                stage_task = TriggerDagRunOperator(
                    task_id=f"trigger_{stage_dag_id(source_label, entity)}",
                    trigger_dag_id=stage_dag_id(source_label, entity),
                    conf=conf,
                    wait_for_completion=True,
                    deferrable=True,
                    poke_interval=15,
                )
                ods_task = TriggerDagRunOperator(
                    task_id=f"trigger_{ods_dag_id(source_label, entity)}",
                    trigger_dag_id=ods_dag_id(source_label, entity),
                    conf=conf,
                    wait_for_completion=True,
                    deferrable=True,
                    poke_interval=15,
                )
                stage_task.set_downstream(ods_task)

            start.set_downstream(entity_group)
            entity_group.set_downstream(all_loads_completed)

    all_loads_completed >> trigger_risk_metrics
