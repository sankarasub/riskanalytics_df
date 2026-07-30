# Repository Map and Configuration Guide

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md) | [Troubleshooting](troubleshooting.md)

This page describes what each major file/folder is used for and how to configure behavior safely.

## Repository Map

### Root

- `docker-compose.yml`: full local platform orchestration and service wiring.
- `README.md`: repository-level guide.
- `mkdocs.yml`: documentation site nav and rendering config.
- `setup_venv.py`: host Python virtual-environment bootstrap and optional library upgrade flow (`--update-libraries`).
- `requirements-lock.txt`: locked dependency snapshot.
- `requirements/dev.txt`: pinned lint/type tooling (`ruff`, `mypy`, type stubs).
- `ruff.toml` and `mypy.ini`: lint and type-check configuration used locally and in CI.
- `.github/workflows/ci.yml`: ruff, mypy, unit tests, `docker compose config`, and Airflow DAG parsing.

### Orchestration

- `airflow/dags/ra_common.py`: shared DAG constants and `spark-submit` command builders.
- `airflow/dags/ra_createtables_and_data.py`: bootstrap DAG (create tables, seed sources, trigger orchestration).
- `airflow/dags/ra_stage_jobs.py`: factory for the eight `ra_<source>_<entity>_stage` DAGs.
- `airflow/dags/ra_ods_jobs.py`: factory for the eight `ra_<source>_<entity>_ods` DAGs.
- `airflow/dags/ra_stage_to_ods_orchestration.py`: STAGE -> ODS -> risk metrics orchestration; one TaskGroup per source/entity, all eight running concurrently with deferred waits.
- `airflow/dags/ra_riskmetrics_eval_ods.py`: final risk metric evaluation DAG.
- `airflow/dags/ra_kafka_streaming.py`: factory for the eight `ra_kafka_<entity>_stage` / `ra_kafka_<entity>_ods` streaming DAGs.

### Jobs

- `jobs/bootstrap.py`: table creation and seed loading.
- `jobs/run_source_to_ods_step.py`: parameterized stage/ODS step execution; `--entity` can be repeated to run several entities in one Spark session.
- `jobs/run_risk_pipeline.py`: final risk pipeline orchestration and publish flow.
- `jobs/risk_pipeline.py`: core Python implementation for risk metric derivation.
- `jobs/kafka_entity_consumer.py`: streaming ingest paths for deals, customer, asset, and collateral topics.
- `jobs/execute_pipeline.py`: ad-hoc runner for a single YAML pipeline definition.

### Runtime package

- `risk_analytics/config.py`: config loader and environment override behavior.
- `risk_analytics/spark.py`: Spark session creation (local/master/connect modes).
- `risk_analytics/nessie.py`: Nessie reference/branch merge interactions.
- `risk_analytics/yaml_executor.py`: YAML pipeline validation, preview, execution.
- `risk_analytics/transformations/`: supported step implementations.
- `risk_analytics/kafka_events.py`: trigger payload builder and the `AwaitMessageSensor` match function.

### Transform metadata

- `transform/`: YAML pipeline definitions.
- `transform/source_to_ods/`: source-to-ODS and risk pipeline YAML contracts.

### Interfaces

- `ui/business_app.py`: business dashboard.
- `ui/developer_app.py`: developer dashboard.
- `api/app.py`: links portal plus operational endpoints (`/health`, `/tables`, `/pipeline/execute`) and YAML pipeline endpoints.

### Operations scripts

Purpose, prerequisites, and step-by-step behavior for each script are documented in
[Scripts Reference](scripts-reference.md).

- `scripts/run_risk_analytics_pipeline.ps1`: end-to-end scripted execution through Airflow, including
  DAG registration preflight, run waits, and final validation.
- `scripts/run_local_python_no_airflow.py`: the same flow without Airflow, plus previews and a run report.
- `scripts/run_manual_pipeline_sequence.ps1`: STAGE/ODS/metrics jobs only, for iterating on YAML changes.
- `scripts/health_check.py`: service reachability probes plus an optional Iceberg query.
- `scripts/validate_pipeline_status.py`: Airflow DAG inventory, Nessie, and ODS data validation from the host.
- `scripts/validate_dags.py`: parses the DAG folder with a real `DagBag` and asserts the expected DAG ids.

### Tests and docs

- `tests/`: unit and behavior tests.
- `docs/`: documentation sources.
- `site/`: built documentation output.

## Configuration Files and What They Control

### `config/platform.yaml`

Controls core behavior:

- Risk constants (`pfe_multiplier`, `var_confidence_z_score`, `default_volatility`)
- Collateral haircuts by asset class
- Catalog and storage defaults
- Pipeline path defaults

### `.env`

Controls runtime credentials and local service settings:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AIRFLOW_UID`
- `AIRFLOW_ADMIN_USER`
- `AIRFLOW_ADMIN_PASSWORD`

### `docker-compose.yml`

Controls:

- Images and build contexts
- Port mappings
- Shared environment blocks
- Named volumes and cache persistence
- Service-level commands for Airflow/Spark/Kafka/UI/API

## Safe Configuration Workflow

1. Copy environment template first:
   - `Copy-Item .env.example .env`
2. Update only values required for your machine or credentials.
3. Keep risk constants in `config/platform.yaml` for transparent behavior.
4. If changing service ports or URLs, update both compose and dependent environment variables.
5. Rebuild only affected services first before full rebuild.

## Common Customization Scenarios

### Change reporting date at runtime

Use script flags or DAG conf, not hard-coded file edits.

### Adjust risk assumptions

Edit `config/platform.yaml` under the `risk` section, then rerun pipeline and compare outputs.

### Add a new transformation

1. Add YAML in `transform/`.
2. Reuse supported component types.
3. Validate/preview through Developer UI or executor APIs.

### Add a new service link in the operations API links portal

Update environment URL variables for `links-api` service and corresponding API/UI mapping.

## Legacy and Transitional Artifacts

Some legacy files and paths may remain while source-to-ODS migration is completed. Use ODS and `risk_metrics` contracts as the primary published data path.

## Production Deployment Reference

For full environment strategy and cloud mappings, use [Production Setup](production_setup.md).

Repository files most relevant for production hardening:

- `docker-compose.yml`: service topology, image tags, ports, and environment contract.
- `docker/airflow/Dockerfile`, `docker/spark/Dockerfile`, `docker/ui/Dockerfile`, `docker/notebook/Dockerfile`: image construction and dependency pinning.
- `.env` / secret-management equivalents: runtime credentials and service connection values.
- `config/platform.yaml`: risk and platform defaults that must be externally governed in production.

Production control checklist from a repo perspective:

1. Pin and promote immutable image tags/digests through CI/CD.
2. Replace local secrets with managed secret injection.
3. Externalize stateful services (catalog metadata, object storage, orchestration metadata, Kafka state).
4. Add monitoring/alerting for Spark jobs, Airflow DAGs, and user-facing services.
5. Separate dev/test/prod catalogs, topics, storage paths, and credentials.
