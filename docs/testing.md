# Test Coverage

> Quick links: [Overview](index.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Repository Map](project-reference.md) | [Troubleshooting](troubleshooting.md)

The repository now has a small unit-test suite built with the Python standard library `unittest` module. The tests are designed to stay lightweight and avoid requiring a live Spark cluster during unit runs.

## What Is Covered

The current coverage includes:

- Configuration loading in [risk_analytics/config.py](../risk_analytics/config.py)
- Nessie REST request shapes in [risk_analytics/nessie.py](../risk_analytics/nessie.py)
- Spark session selection in [risk_analytics/spark.py](../risk_analytics/spark.py)
- Pipeline orchestration in [jobs/run_risk_pipeline.py](../jobs/run_risk_pipeline.py)
- Bootstrap table creation and seeding in [jobs/bootstrap.py](../jobs/bootstrap.py)

## What Each Test Verifies

- `tests/test_config.py`
  - Reads `config/platform.yaml`
  - Applies `NESSIE_URI` and `S3_ENDPOINT` overrides
  - Preserves the rest of the config structure

- `tests/test_nessie.py`
  - Calls `GET /trees` for branch lookup
  - Calls `GET /trees/main` before branch creation
  - Sends the expected `POST /trees/branch` payload
  - Sends the expected merge request payload to `POST /trees/branch/main/merge`

- `tests/test_spark_session.py`
  - Uses Spark Connect when `SPARK_REMOTE` is set
  - Builds a local Spark session when `SPARK_REMOTE` is not set
  - Applies the Iceberg and Nessie Spark configs used by the project

- `tests/test_run_risk_pipeline.py`
  - Creates the branch used for an isolated risk run
  - Calls the Spark session builder with the branch ref
  - Runs the risk-metric calculation step
  - Writes to `nessie.risk_analytics_ods.risk_metrics` (source-to-ODS model)
  - Merges the branch back to `main`

- `tests/test_bootstrap.py`
  - Creates the namespace and all Iceberg tables
  - Seeds source tables only when they are empty
  - Rejects seed attempts for tables that are create-only

## How To Run

Run the suite from the repository root:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py"
```

## Not Yet Covered

These are still integration gaps rather than unit gaps:

- End-to-end Spark SQL execution against real Iceberg tables
- Docker Compose startup and inter-service health checks
- Airflow DAG execution inside the Airflow scheduler/webserver stack
- GitHub Pages and MkDocs deployment behavior

Those are better handled with a separate integration test layer or smoke-test scripts against the running stack.

