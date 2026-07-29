"""Fail if any Risk Analytics DAG does not import or is missing from the DagBag.

Runs the same parse the Airflow scheduler performs, so import errors and missing
DAG ids are caught in CI instead of on a running platform.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_FOLDER = REPO_ROOT / "airflow" / "dags"

ENTITIES = ("customer", "asset", "collateral", "deals")
SOURCE_LABELS = ("sourceA", "sourceB")


def expected_dag_ids() -> set[str]:
    expected = {
        "ra_createtables_and_data",
        "ra_stage_to_ods_orchestration",
        "ra_riskmetrics_eval_ods",
    }
    for entity in ENTITIES:
        expected.add(f"ra_kafka_{entity}_stage")
        expected.add(f"ra_kafka_{entity}_ods")
        for source in SOURCE_LABELS:
            expected.add(f"ra_{source}_{entity}_stage")
            expected.add(f"ra_{source}_{entity}_ods")
    return expected


def main() -> int:
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    sys.path.insert(0, str(REPO_ROOT))

    from airflow.models import DagBag

    dag_bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)

    if dag_bag.import_errors:
        for path, error in dag_bag.import_errors.items():
            print(f"[dag-import-error] {path}\n{error}", file=sys.stderr)
        return 1

    missing = sorted(expected_dag_ids() - set(dag_bag.dags))
    if missing:
        print(f"[dag-missing] {', '.join(missing)}", file=sys.stderr)
        return 1

    untriggered = sorted(dag_id for dag_id, dag in dag_bag.dags.items() if not dag.tasks)
    if untriggered:
        print(f"[dag-without-tasks] {', '.join(untriggered)}", file=sys.stderr)
        return 1

    print(f"Parsed {len(dag_bag.dags)} DAGs with no import errors.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
