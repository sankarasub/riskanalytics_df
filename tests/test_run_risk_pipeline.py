from __future__ import annotations

import importlib
import sys
import unittest
from unittest.mock import Mock, patch

from tests.support import build_fake_pyspark


class RunRiskPipelineTests(unittest.TestCase):
    def test_main_creates_branch_runs_job_and_merges(self) -> None:
        fake_modules, _builder = build_fake_pyspark()
        sys.modules.pop("jobs.run_risk_pipeline", None)

        with patch.dict(sys.modules, fake_modules, clear=False):
            module = importlib.import_module("jobs.run_risk_pipeline")

        cfg = {
            "catalog": {"nessie_uri": "http://nessie:19120/api/v2"},
            "executor": {"pipeline_path": "transform/risk_metrics_pipeline.yaml"},
        }
        nessie = Mock()
        nessie.branch_exists.return_value = False
        spark = Mock()
        execution = Mock()
        execution.target_row_counts = {"risk_metrics": 9}

        with (
            patch.object(module, "load_config", return_value=cfg),
            patch.object(module, "NessieClient", return_value=nessie) as nessie_client_mock,
            patch.object(module, "create_spark_session", return_value=spark) as create_spark_session_mock,
            patch.object(module, "run_pipeline_from_yaml", return_value=execution) as run_pipeline_from_yaml_mock,
        ):
            module.main("2026-07-18", "12345678-1234-1234-1234-123456789abc")

        expected_branch = "risk-run-123456781234"
        nessie_client_mock.assert_called_once_with("http://nessie:19120/api/v2")
        nessie.branch_exists.assert_called_once_with(expected_branch)
        nessie.create_branch.assert_called_once_with(expected_branch)
        create_spark_session_mock.assert_called_once_with("risk-analytics-risk-pipeline", ref=expected_branch)
        run_pipeline_from_yaml_mock.assert_called_once_with(
            spark=spark,
            pipeline_path="transform/risk_metrics_pipeline.yaml",
            config=cfg,
            runtime_params={
                "as_of_date": "2026-07-18",
                "risk_run_id": "12345678-1234-1234-1234-123456789abc",
                "source_branch": expected_branch,
                "data_model": "legacy",
            },
        )
        spark.stop.assert_called_once_with()
        nessie.merge.assert_called_once_with(expected_branch, "main")

    def test_main_uses_source_to_ods_pipeline_when_requested(self) -> None:
        fake_modules, _builder = build_fake_pyspark()
        sys.modules.pop("jobs.run_risk_pipeline", None)

        with patch.dict(sys.modules, fake_modules, clear=False):
            module = importlib.import_module("jobs.run_risk_pipeline")

        cfg = {
            "catalog": {"nessie_uri": "http://nessie:19120/api/v2"},
            "executor": {"pipeline_path": "transform/risk_metrics_pipeline.yaml"},
        }
        nessie = Mock()
        nessie.branch_exists.return_value = True
        spark = Mock()
        execution = Mock()
        execution.target_row_counts = {"risk_metrics": 3}

        with (
            patch.object(module, "load_config", return_value=cfg),
            patch.object(module, "NessieClient", return_value=nessie),
            patch.object(module, "create_spark_session", return_value=spark),
            patch.object(module, "run_pipeline_from_yaml", return_value=execution) as run_pipeline_from_yaml_mock,
        ):
            module.main("2026-07-18", "12345678-1234-1234-1234-123456789abc", "source-to-ods")

        pipeline_path = run_pipeline_from_yaml_mock.call_args.kwargs["pipeline_path"]
        self.assertTrue(
            pipeline_path.replace("\\", "/").endswith("transform/source_to_ods/risk_metrics_pipeline_source_to_ods.yaml")
        )

