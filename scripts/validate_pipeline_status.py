#!/usr/bin/env python3
"""Post-run validation of the Risk Analytics platform from the host.

Why this script exists
----------------------
`scripts/health_check.py` answers "are the services reachable?". This script
answers the next question: "is the platform *configured and loaded* correctly?".
It is the script to run after `docker compose up -d` or after a pipeline run,
and it needs no Airflow container exec access - everything goes through the REST
APIs and Spark Connect from the host.

What it does, in order
----------------------
1. Airflow: authenticates against the REST API (trying the local credential
   candidates) and compares the registered DAG inventory against the 27 `ra_*`
   DAGs this repository ships. Missing DAGs mean the mounted `airflow/dags`
   folder is stale; leftover `risk_analytics_*` DAGs mean pre-refactor metadata
   rows survived in the Airflow database.
2. Nessie: confirms the `main` branch is readable over the v2 REST API.
3. Spark: opens a Spark Connect session, lists `nessie.risk_analytics_ods`, and
   counts `risk_metrics`. The table only exists after `ra_riskmetrics_eval_ods`
   has run, so those two steps warn instead of failing.

Exit code is 0 only when Airflow, Nessie, and Spark all pass.

Usage:
  python scripts/validate_pipeline_status.py
"""
from __future__ import annotations

import sys

import requests

ENTITIES = ("customer", "asset", "collateral", "deals")
SOURCE_LABELS = ("sourceA", "sourceB")


def expected_dag_ids() -> set[str]:
    """Return every DAG id the repository ships (3 platform + 16 batch + 8 Kafka)."""
    dag_ids = {"ra_createtables_and_data", "ra_stage_to_ods_orchestration", "ra_riskmetrics_eval_ods"}
    for source in SOURCE_LABELS:
        for entity in ENTITIES:
            dag_ids.add(f"ra_{source}_{entity}_stage")
            dag_ids.add(f"ra_{source}_{entity}_ods")
    for entity in ENTITIES:
        dag_ids.add(f"ra_kafka_{entity}_stage")
        dag_ids.add(f"ra_kafka_{entity}_ods")
    return dag_ids


def http_get(url, auth=None, timeout=10):
    try:
        r = requests.get(url, auth=auth, timeout=timeout)
        return r.status_code, r.text
    except Exception as e:
        return None, str(e)


def try_airflow_auth(url, candidates):
    """Return the first credential pair that gets a non-401 answer from Airflow."""
    for user, password in candidates:
        status, text = http_get(url, auth=(user, password), timeout=10)
        if status == 200:
            return user, password, status, text
        if status is not None and status != 401:
            return user, password, status, text
    return None, None, None, None


def check_airflow():
    print("[AIRFLOW] Checking Airflow API and the registered DAG inventory...")
    candidates = [("admin", "admin"), ("airflow", "airflow"), ("admin", "airflow")]
    user, password, status, text = try_airflow_auth("http://localhost:8088/api/v1/dags?limit=500", candidates)
    if status is None:
        print("[FAIL] Could not reach Airflow API")
        return False
    if status != 200:
        print(f"[FAIL] Airflow API returned {status}")
        print(text[:1000])
        return False

    print("[PASS] Airflow API reachable")
    dags = requests.get(
        "http://localhost:8088/api/v1/dags?limit=500", auth=(user, password), timeout=10
    ).json()["dags"]
    registered = {dag["dag_id"] for dag in dags}

    expected = expected_dag_ids()
    missing = sorted(expected - registered)
    if missing:
        print(f"[FAIL] Airflow is missing {len(missing)} expected DAG(s): {', '.join(missing)}")
        print("[INFO] The mounted airflow/dags folder is stale: run 'git pull' then 'docker compose up -d'")
        return False
    print(f"[PASS] All {len(expected)} ra_* DAGs are registered")

    # Metadata rows for deleted DAG files keep showing up in the UI and can be
    # triggered by stale automation, so surface them explicitly.
    legacy = sorted(dag_id for dag_id in registered if dag_id.startswith("risk_analytics"))
    if legacy:
        print(f"[WARN] Pre-refactor DAG metadata still registered: {', '.join(legacy)}")
        print("[INFO] Remove it with: .\\scripts\\run_risk_analytics_pipeline.ps1 -RemoveLegacyDags")

    paused = sorted(dag["dag_id"] for dag in dags if dag["dag_id"] in expected and dag.get("is_paused"))
    if paused:
        print(f"[WARN] {len(paused)} expected DAG(s) are paused: {', '.join(paused)}")
    return True


def check_nessie():
    print("\n[NESSIE] Checking Nessie API...")
    status, text = http_get("http://localhost:19120/api/v2/trees/main", timeout=10)
    if status == 200:
        print("[PASS] Nessie API reachable")
        print(text[:1000])
        return True
    print(f"[FAIL] Nessie API returned {status}: {text}")
    return False


def check_spark():
    print("\n[SPARK] Checking Spark Connect and the ODS layer...")
    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        print(f"[FAIL] PySpark not available: {e}")
        return False

    try:
        spark = SparkSession.builder.remote("sc://localhost:15002").appName("validate-pipeline-status").getOrCreate()
        print("[PASS] Spark Connect session created")
        try:
            tables = spark.sql("SHOW TABLES IN nessie.risk_analytics_ods").collect()
            print("Tables:", tables)
            try:
                # risk_metrics only exists once ra_riskmetrics_eval_ods has run.
                count = spark.sql("SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics").collect()[0]["c"]
                print("risk_metrics count:", count)
            except Exception as e:
                print(f"[WARN] risk_metrics query failed: {e}")
                print("[INFO] Spark is up; run ra_riskmetrics_eval_ods (or the helper script) to publish metrics")
        except Exception as e:
            print(f"[WARN] Table listing failed: {e}")
            print("[INFO] Spark connection is up, but the Iceberg catalog view may still be unavailable")
        spark.stop()
        return True
    except Exception as e:
        print(f"[FAIL] Spark query failed: {e}")
        return False


def main():
    results = []
    results.append(("Airflow", check_airflow()))
    results.append(("Nessie", check_nessie()))
    results.append(("Spark", check_spark()))

    print("\nSUMMARY")
    print("-------")
    for name, ok in results:
        print(f"{name}: {'PASS' if ok else 'FAIL'}")

    if all(ok for _, ok in results):
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
