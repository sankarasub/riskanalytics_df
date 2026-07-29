from __future__ import annotations

import unittest
from pathlib import Path


class AirflowDockerfileTests(unittest.TestCase):
    def test_airflow_image_installs_java_and_sets_java_home(self) -> None:
        dockerfile_path = Path(__file__).resolve().parents[1] / "docker" / "airflow" / "Dockerfile"
        dockerfile_text = dockerfile_path.read_text(encoding="utf-8")

        self.assertIn("openjdk-17-jdk-headless", dockerfile_text)
        self.assertIn("JAVA_HOME", dockerfile_text)

    def test_spark_submit_command_builders_clear_spark_remote(self) -> None:
        shared_module = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "ra_common.py"
        content = shared_module.read_text(encoding="utf-8")

        self.assertIn("env -u SPARK_REMOTE", content)
        self.assertIn("spark-submit --master local[*]", content)


if __name__ == "__main__":
    unittest.main()
