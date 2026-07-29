# Test Coverage

> Quick links: [Overview](index.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Repository Map](project-reference.md) | [Troubleshooting](troubleshooting.md)

The repository has a unit-test suite built with the Python standard library `unittest` module, plus lint, type, DAG-parse, and docs gates that run in CI. The tests are designed to stay lightweight and avoid requiring a live Spark cluster during unit runs.

## What Is Covered

The current coverage includes:

- Configuration loading in [risk_analytics/config.py](../risk_analytics/config.py)
- Nessie REST request shapes in [risk_analytics/nessie.py](../risk_analytics/nessie.py)
- Spark session selection in [risk_analytics/spark.py](../risk_analytics/spark.py)
- Kafka trigger payloads and the sensor matcher in [risk_analytics/kafka_events.py](../risk_analytics/kafka_events.py)
- YAML transformation execution in [risk_analytics/yaml_executor.py](../risk_analytics/yaml_executor.py)
- Pipeline orchestration in [jobs/run_risk_pipeline.py](../jobs/run_risk_pipeline.py)
- The multi-entity layer runner in [jobs/run_source_to_ods_step.py](../jobs/run_source_to_ods_step.py)
- Bootstrap table creation and seeding in [jobs/bootstrap.py](../jobs/bootstrap.py)
- Airflow DAG ids, dependencies, pools, and Kafka sensor wiring in [airflow/dags](../airflow/dags)
- Operations API request handling in [api/app.py](../api/app.py)

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

- `tests/test_airflow_ra_dags.py`
  - Registers every required DAG id, including the eight batch STAGE/ODS pairs and the eight Kafka DAGs
  - Asserts the STAGE-before-ODS and ODS-before-risk-metrics dependencies
  - Asserts every spark-submit task clears `SPARK_REMOTE` and uses the `spark_submit` pool
  - Asserts the Kafka sensors filter on their own entity and use the importable matcher path
  - Asserts the orchestration triggers wait in deferred state

- `tests/test_kafka_events.py`
  - Matches an event for the sensor's entity and ignores other entities
  - Keeps waiting on empty, non-JSON, non-object, and date-less messages
  - Round-trips the payload the streaming job publishes

- `tests/test_run_source_to_ods_step.py`
  - Resolves the YAML pipeline per entity and layer
  - Runs repeated `--entity` flags in one Spark session, de-duplicated and in order
  - Fails on a missing YAML definition before starting Spark

- `tests/test_api_triggers.py`
  - Validates the `/pipeline/execute` targets and rejects unknown entities and sources

## How To Run

Run the suite from the repository root:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -t .
```

Lint and type-check with the pinned tooling in `requirements/dev.txt`:

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\mypy.exe --config-file mypy.ini
```

Parse the DAGs the way the Airflow scheduler does (needs `requirements/airflow.txt` installed):

```powershell
$env:PYTHONPATH = (Get-Location).Path
.\.venv\Scripts\python.exe scripts\validate_dags.py
```

## Continuous Integration

[.github/workflows/ci.yml](../.github/workflows/ci.yml) runs on pull requests and pushes to `main`:

- `ruff check .`
- `mypy --config-file mypy.ini`
- `python -m unittest discover -s tests -p "test_*.py" -t .`
- `docker compose config --quiet`
- `python scripts/validate_dags.py` in a separate job with Airflow installed, which fails on any import error, missing DAG id, or task-less DAG

[.github/workflows/docs-pr-check.yml](../.github/workflows/docs-pr-check.yml) runs `mkdocs build --strict` for documentation changes.

## Not Yet Covered

These are still integration gaps rather than unit gaps:

- End-to-end Spark SQL execution against real Iceberg tables
- Docker Compose startup and inter-service health checks
- Airflow DAG execution inside the Airflow scheduler/webserver stack (CI parses the DAGs but does not run them)
- Kafka end-to-end delivery from the streaming job through the sensors
- GitHub Pages and MkDocs deployment behavior

Those are better handled with a separate integration test layer or smoke-test scripts against the running stack.
