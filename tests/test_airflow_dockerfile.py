from __future__ import annotations

import unittest
from pathlib import Path


class AirflowDockerfileTests(unittest.TestCase):
    def test_airflow_image_installs_java_and_sets_java_home(self) -> None:
        dockerfile_path = Path(__file__).resolve().parents[1] / "docker" / "airflow" / "Dockerfile"
        dockerfile_text = dockerfile_path.read_text(encoding="utf-8")

        self.assertIn("openjdk-17-jdk-headless", dockerfile_text)
        self.assertIn("JAVA_HOME", dockerfile_text)

    def test_airflow_dag_commands_clear_spark_remote_for_spark_submit(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        dag_files = [
            repo_root / "airflow" / "dags" / "risk_analytics_create_tables_and_load_data.py",
            repo_root / "airflow" / "dags" / "risk_analytics_pipeline.py",
            repo_root / "airflow" / "dags" / "risk_analytics_source_transforms.py",
            repo_root / "airflow" / "dags" / "risk_analytics_source_to_ods.py",
        ]

        for dag_file in dag_files:
            content = dag_file.read_text(encoding="utf-8")
            self.assertIn("env -u SPARK_REMOTE", content)
            self.assertIn("spark-submit --master local[*]", content)


if __name__ == "__main__":
    unittest.main()
