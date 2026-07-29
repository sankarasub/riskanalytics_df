from __future__ import annotations

import unittest
from pathlib import Path


class AirflowSourceToOdsOrchestrationTests(unittest.TestCase):
    def test_source_to_ods_orchestration_triggers_risk_pipeline_with_data_model(self) -> None:
        dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "risk_analytics_source_to_ods.py"
        content = dag_path.read_text(encoding="utf-8")

        self.assertIn("trigger_dag_id=\"risk_analytics_pipeline\"", content)
        self.assertIn("\"data_model\": \"source-to-ods\"", content)

    def test_kafka_listener_targets_source_to_ods_orchestration(self) -> None:
        dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "risk_analytics_kafka_listener.py"
        content = dag_path.read_text(encoding="utf-8")

        self.assertIn("trigger_dag_id=\"risk_analytics_kafka_entity_orchestration\"", content)
        self.assertIn("\"data_model\": \"source-to-ods\"", content)

    def test_pipeline_dag_accepts_data_model_argument(self) -> None:
        dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "risk_analytics_pipeline.py"
        content = dag_path.read_text(encoding="utf-8")

        self.assertIn("DATA_MODEL", content)
        self.assertIn("--data-model", content)


if __name__ == "__main__":
    unittest.main()
