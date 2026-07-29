from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.support import build_fake_airflow

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_MODULES = (
    "ra_createtables_and_data",
    "ra_stage_jobs",
    "ra_ods_jobs",
    "ra_riskmetrics_eval_ods",
    "ra_stage_to_ods_orchestration",
    "ra_kafka_streaming",
)
ENTITIES = ("customer", "asset", "collateral", "deals")
SOURCE_LABELS = ("sourceA", "sourceB")


def _load_all_dags() -> dict:
    """Import every DAG module against Airflow stubs and return the DAG registry."""
    fake_modules, collected = build_fake_airflow()
    sys.path.insert(0, str(DAGS_DIR))
    try:
        with patch.dict(sys.modules, fake_modules, clear=False):
            for module_name in DAG_MODULES:
                sys.modules.pop(module_name, None)
                importlib.import_module(module_name)
    finally:
        sys.path.remove(str(DAGS_DIR))
        for module_name in DAG_MODULES:
            sys.modules.pop(module_name, None)
    return collected


class RiskAnalyticsDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dags = _load_all_dags()

    def test_all_required_dag_ids_are_registered(self) -> None:
        expected = {"ra_createtables_and_data", "ra_stage_to_ods_orchestration", "ra_riskmetrics_eval_ods", "ra_kafka_customer_stage", "ra_kafka_customer_ods"}
        for source in SOURCE_LABELS:
            for entity in ENTITIES:
                expected.add(f"ra_{source}_{entity}_stage")
                expected.add(f"ra_{source}_{entity}_ods")

        self.assertTrue(expected.issubset(set(self.dags)), msg=f"missing: {sorted(expected - set(self.dags))}")

    def test_stage_and_ods_dags_run_their_yaml_driven_layer(self) -> None:
        for source, source_key in (("sourceA", "sourcea"), ("sourceB", "sourceb")):
            for entity in ENTITIES:
                stage = self.dags[f"ra_{source}_{entity}_stage"].tasks[f"load_{entity}_stage"]
                ods = self.dags[f"ra_{source}_{entity}_ods"].tasks[f"load_{entity}_ods"]
                for task, layer in ((stage, "stage"), (ods, "ods")):
                    command = task.kwargs["bash_command"]
                    self.assertIn(f"--layer {layer}", command)
                    self.assertIn(f"--entity {entity}", command)
                    self.assertIn(f"--source {source_key}", command)
                    self.assertIn("run_source_to_ods_step.py", command)

    def test_stage_and_ods_dags_declare_dependencies(self) -> None:
        for source in SOURCE_LABELS:
            for entity in ENTITIES:
                for dag_id, task_id in (
                    (f"ra_{source}_{entity}_stage", f"load_{entity}_stage"),
                    (f"ra_{source}_{entity}_ods", f"load_{entity}_ods"),
                ):
                    task = self.dags[dag_id].tasks[task_id]
                    self.assertTrue(task.upstream, msg=f"{dag_id}.{task_id} has no upstream")
                    self.assertTrue(task.downstream, msg=f"{dag_id}.{task_id} has no downstream")

    def test_bootstrap_dag_seeds_after_table_creation_then_triggers_orchestration(self) -> None:
        dag = self.dags["ra_createtables_and_data"]
        create = dag.tasks["create_all_tables"]
        seed_customer = dag.tasks["seed_customer_table"]
        completed = dag.tasks["all_source_tables_seeded"]
        trigger = dag.tasks["trigger_ra_stage_to_ods_orchestration"]

        self.assertIn(seed_customer, create.downstream)
        self.assertIn(completed, seed_customer.downstream)
        self.assertIn(trigger, completed.downstream)
        self.assertEqual(trigger.kwargs["trigger_dag_id"], "ra_stage_to_ods_orchestration")

    def test_orchestration_runs_stage_before_ods_then_risk_metrics(self) -> None:
        dag = self.dags["ra_stage_to_ods_orchestration"]
        completed = dag.tasks["all_stage_and_ods_loads_completed"]
        metrics = dag.tasks["trigger_ra_riskmetrics_eval_ods"]

        for source in SOURCE_LABELS:
            for entity in ENTITIES:
                stage = dag.tasks[f"trigger_ra_{source}_{entity}_stage"]
                ods = dag.tasks[f"trigger_ra_{source}_{entity}_ods"]
                self.assertIn(ods, stage.downstream)
                self.assertIn(completed, ods.downstream)
                self.assertTrue(stage.kwargs["wait_for_completion"])
                self.assertTrue(ods.kwargs["wait_for_completion"])

        self.assertIn(metrics, completed.downstream)
        self.assertEqual(metrics.kwargs["trigger_dag_id"], "ra_riskmetrics_eval_ods")

    def test_kafka_dags_trigger_risk_metrics_directly(self) -> None:
        stage_dag = self.dags["ra_kafka_customer_stage"]
        ods_dag = self.dags["ra_kafka_customer_ods"]

        sensor = stage_dag.tasks["wait_for_customer_event"]
        load = stage_dag.tasks["load_customer_stage"]
        trigger_ods = stage_dag.tasks["trigger_ra_kafka_customer_ods"]
        stage_metrics = stage_dag.tasks["trigger_ra_riskmetrics_eval_ods"]

        self.assertIn(load, sensor.downstream)
        self.assertIn(trigger_ods, load.downstream)
        self.assertIn(stage_metrics, trigger_ods.downstream)
        self.assertEqual(trigger_ods.kwargs["trigger_dag_id"], "ra_kafka_customer_ods")
        self.assertEqual(stage_metrics.kwargs["trigger_dag_id"], "ra_riskmetrics_eval_ods")
        self.assertFalse(trigger_ods.kwargs["conf"]["trigger_riskmetrics"])

        ods_metrics = ods_dag.tasks["trigger_ra_riskmetrics_eval_ods"]
        self.assertEqual(ods_metrics.kwargs["trigger_dag_id"], "ra_riskmetrics_eval_ods")
        self.assertIn(ods_metrics, ods_dag.tasks["check_riskmetrics_trigger_requested"].downstream)

    def test_risk_metrics_dag_runs_the_source_to_ods_data_model(self) -> None:
        command = self.dags["ra_riskmetrics_eval_ods"].tasks["evaluate_and_publish_risk_metrics"].kwargs["bash_command"]

        self.assertIn("run_risk_pipeline.py", command)
        self.assertIn("--data-model", command)
        self.assertIn("source-to-ods", command)

    def test_dag_commands_clear_spark_remote_for_spark_submit(self) -> None:
        for dag_file in sorted(DAGS_DIR.glob("ra_*.py")):
            content = dag_file.read_text(encoding="utf-8")
            if "spark-submit" not in content:
                continue
            self.assertIn("env -u SPARK_REMOTE", content)
            self.assertIn("spark-submit --master local[*]", content)


if __name__ == "__main__":
    unittest.main()
