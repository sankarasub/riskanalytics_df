from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SCRIPT = REPO_ROOT / "scripts" / "run_risk_analytics_pipeline.ps1"
LEGACY_DAG_IDS = (
    "risk_analytics_create_tables_and_load_data",
    "risk_analytics_source_to_ods_orchestration",
    "risk_analytics_kafka_customer_stage",
    "risk_analytics_kafka_customer_ods",
    "risk_analytics_risk_pipeline",
)
EXECUTABLE_DIRS = ("airflow", "api", "jobs", "risk_analytics", "scripts", "ui")


class PipelineScriptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.script = PIPELINE_SCRIPT.read_text(encoding="utf-8")

    def test_script_triggers_the_current_bootstrap_dag(self) -> None:
        self.assertIn("Invoke-AirflowDagTrigger -DagId 'ra_createtables_and_data'", self.script)

    def test_script_covers_every_shipped_dag(self) -> None:
        for fragment in (
            "'ra_createtables_and_data', 'ra_stage_to_ods_orchestration', 'ra_riskmetrics_eval_ods'",
            '"ra_${source}_${entity}_stage"',
            '"ra_${source}_${entity}_ods"',
            '"ra_kafka_${entity}_stage"',
            '"ra_kafka_${entity}_ods"',
        ):
            self.assertIn(fragment, self.script)

    def test_script_validates_registration_before_triggering(self) -> None:
        registration_index = self.script.index("$registeredDagIds = Assert-ExpectedDagsRegistered")
        trigger_index = self.script.index("Invoke-AirflowDagTrigger -DagId 'ra_createtables_and_data'")
        self.assertLess(registration_index, trigger_index)

    def test_script_waits_for_the_pipeline_before_validating(self) -> None:
        wait_index = self.script.index("Wait-ForTriggeredDagRun -DagId $dagId")
        validation_index = self.script.rindex("Invoke-ValidationQuery")
        self.assertLess(wait_index, validation_index)

    def test_no_executable_file_references_legacy_dag_ids(self) -> None:
        offenders = []
        for directory in EXECUTABLE_DIRS:
            for path in sorted((REPO_ROOT / directory).rglob("*")):
                if not path.is_file() or path.suffix not in {".py", ".ps1", ".yaml", ".yml", ".sh"}:
                    continue
                text = path.read_text(encoding="utf-8", errors="ignore")
                for legacy_dag_id in LEGACY_DAG_IDS:
                    if legacy_dag_id in text and "airflow dags delete" not in text:
                        offenders.append(f"{path.relative_to(REPO_ROOT)}: {legacy_dag_id}")
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
