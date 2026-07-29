from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.support import build_fake_pyspark

REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeSpark:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class RunSourceToOdsStepTests(unittest.TestCase):
    def _import_step_job(self):
        fake_modules, _ = build_fake_pyspark()
        sys.path.insert(0, str(REPO_ROOT / "jobs"))
        try:
            with patch.dict(sys.modules, fake_modules, clear=False):
                sys.modules.pop("run_source_to_ods_step", None)
                module = importlib.import_module("run_source_to_ods_step")
        finally:
            sys.path.remove(str(REPO_ROOT / "jobs"))
            sys.modules.pop("run_source_to_ods_step", None)
        return module

    def _run(self, argv: list[str]):
        module = self._import_step_job()
        spark = FakeSpark()
        with (
            patch.object(module, "load_config", return_value={"catalog": {"name": "nessie"}}),
            patch.object(module, "create_spark_session", return_value=spark),
            patch.object(module, "run_pipeline_from_yaml") as run_mock,
            patch.object(sys, "argv", ["run_source_to_ods_step.py", *argv]),
        ):
            module.main()
        return spark, run_mock

    def test_single_entity_runs_its_yaml_definition(self) -> None:
        spark, run_mock = self._run(
            ["--layer", "stage", "--entity", "customer", "--source", "sourcea", "--as-of-date", "2026-07-18"]
        )

        run_mock.assert_called_once()
        kwargs = run_mock.call_args.kwargs
        self.assertEqual(Path(kwargs["pipeline_path"]).name, "stage_customer_sourcea.yaml")
        self.assertEqual(kwargs["runtime_params"]["entity"], "customer")
        self.assertEqual(kwargs["runtime_params"]["as_of_date"], "2026-07-18")
        self.assertTrue(spark.stopped)

    def test_repeated_entities_share_one_spark_session(self) -> None:
        spark, run_mock = self._run(
            [
                "--layer",
                "ods",
                "--entity",
                "customer",
                "--entity",
                "asset",
                "--entity",
                "customer",
                "--source",
                "sourceb",
                "--param",
                "customer_sourceb_path=/data/customer/*.csv",
            ]
        )

        pipelines = [Path(call.kwargs["pipeline_path"]).name for call in run_mock.call_args_list]
        entities = [call.kwargs["runtime_params"]["entity"] for call in run_mock.call_args_list]

        self.assertEqual(pipelines, ["ods_customer_sourceb.yaml", "ods_asset_sourceb.yaml"])
        self.assertEqual(entities, ["customer", "asset"])
        for call in run_mock.call_args_list:
            self.assertEqual(call.kwargs["spark"], spark)
            self.assertEqual(
                call.kwargs["runtime_params"]["customer_sourceb_path"],
                "/data/customer/*.csv",
            )
        self.assertTrue(spark.stopped)

    def test_missing_pipeline_definition_fails_before_spark_starts(self) -> None:
        module = self._import_step_job()
        with (
            patch.object(module, "create_spark_session") as session_mock,
            patch.object(module, "_pipeline_path", side_effect=FileNotFoundError("missing")),
            patch.object(sys, "argv", ["run_source_to_ods_step.py", "--layer", "stage", "--entity", "deals", "--source", "sourcea"]),
            self.assertRaises(FileNotFoundError),
        ):
            module.main()
        session_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
