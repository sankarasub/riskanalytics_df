# Repository File Guide and Configuration

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Data Model](data-model-risk-metrics.md) | [Scripts](scripts-reference.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md) | [Troubleshooting](troubleshooting.md)

This page is the file-level inventory: every tracked file or folder, why it exists, and when you
touch it. It then explains the configuration surface. Use
[Architecture](architecture.md) for how the pieces interact and
[Scripts Reference](scripts-reference.md) for the operational scripts in detail.

## Root Files

| File | Purpose | When you touch it |
| --- | --- | --- |
| `README.md` | Repository entry point: quick start, service URLs, documentation index. | Whenever the quick start or documentation set changes. |
| `docker-compose.yml` | Defines every service, image, port, volume, and environment variable of the local platform. | Adding a service, changing a port, or changing an environment contract. |
| `mkdocs.yml` | Documentation site navigation, theme, and Markdown extensions (Mermaid, snippets). | Adding, renaming, or removing a page under `docs/`. |
| `config/platform.yaml` | Default catalog/storage/risk configuration consumed by every job. | Changing risk assumptions, namespaces, or the default pipeline path. |
| `setup_venv.py` | Creates the host `.venv` and installs every requirement group in one resolve, then verifies local-mode imports. | Changing host dependencies or the bootstrap flow. |
| `requirements-lock.txt` | Snapshot of a known-good resolved dependency set for reproducible host installs. | Refreshing pins after a dependency change. |
| `ruff.toml` | Lint rules, per-file ignores, and the reasons for each ignore. | Adding a file that legitimately needs an exception. |
| `mypy.ini` | Type-check scope and per-module strictness. | Adding a module to type checking. |
| `.env.example` | Template for `.env`: S3 credentials, Airflow UID, and Airflow admin user. Copy it; never commit `.env`. | Adding a new runtime secret or setting. |
| `.gitignore` | Keeps build output, local data volumes, secrets, and caches out of the repository. | Adding generated artefacts. |
| `.gitattributes` | Normalizes line endings, which matters because the runner scripts are PowerShell. | Rarely. |

## Requirements (`requirements/`)

Each image installs only its own group, so a UI change cannot break Spark. Cross-cutting pins
(`grpcio`, `protobuf`, `pyarrow`) are kept identical across groups on purpose - Spark Connect fails
at runtime when the client and server disagree.

| File | Installed into |
| --- | --- |
| `requirements/spark.txt` | Spark master/worker/connect and the streaming consumer image. |
| `requirements/airflow.txt` | Airflow webserver, scheduler, triggerer, and init (pins `apache-airflow` so the Kafka provider cannot pull Airflow 3). |
| `requirements/ui.txt` | Streamlit UIs and the FastAPI operations API. |
| `requirements/notebook.txt` | JupyterLab image. |
| `requirements/docs.txt` | MkDocs and the Material theme, used locally and by the docs workflows. |
| `requirements/dev.txt` | `ruff`, `mypy`, and type stubs; installed on the host by `setup_venv.py`. |

## Airflow DAGs (`airflow/dags/`)

The folder ships 27 DAGs; the factories generate most of them, so the file count stays small. See
[Batch Orchestration Flow](architecture.md#batch-orchestration-flow) for the generated DAG ids.

| File | Purpose |
| --- | --- |
| `ra_common.py` | Single source of truth for entities, sources, namespaces, pool name, and the `spark-submit` command builders every DAG uses. Change a job argument here, not in seven DAG files. |
| `ra_createtables_and_data.py` | Bootstrap DAG: create namespaces and tables, seed deterministic source data, then trigger the orchestration DAG. |
| `ra_stage_jobs.py` | Factory for the eight `ra_<source>_<entity>_stage` DAGs (one parameterized STAGE load each). |
| `ra_ods_jobs.py` | Factory for the eight `ra_<source>_<entity>_ods` DAGs (one parameterized ODS merge each). |
| `ra_stage_to_ods_orchestration.py` | Fans out into one TaskGroup per source/entity pair, all eight concurrent with deferred waits, then triggers the metrics DAG. |
| `ra_riskmetrics_eval_ods.py` | Runs the branch-isolated risk job and publishes `risk_metrics`. |
| `ra_kafka_streaming.py` | Factory for the eight `ra_kafka_<entity>_stage` / `ra_kafka_<entity>_ods` DAGs, including the entity-filtered `AwaitMessageSensor`. |

## Spark Jobs (`jobs/`)

Every job is a `spark-submit` entry point with an explicit CLI, so Airflow, the scripts, and a
developer shell all invoke the same code path.

| File | Purpose |
| --- | --- |
| `bootstrap.py` | Creates all table contracts and loads the deterministic seed data from `data/sourcea`. Idempotent. |
| `create_tables.py` | Holds the DDL for every table (source, stage, ODS, metrics) that `bootstrap.py` executes. |
| `run_source_to_ods_step.py` | Runs one STAGE or ODS step for a source/entity/date; `--entity` is repeatable so several entities share one Spark session. |
| `run_risk_pipeline.py` | Creates the Nessie run branch, executes the risk metrics YAML, merges to `main`, and publishes the completion event. |
| `kafka_entity_consumer.py` | Structured Streaming consumer behind the `kafka-entity-stream` service: lands ingest topics into source tables and emits trigger events. |
| `execute_pipeline.py` | Ad-hoc runner for any YAML pipeline file, used for validation and previews outside orchestration. |
| `__init__.py` | Makes `jobs` importable so `bootstrap.py` can import the shared DDL. |

## Runtime Package (`risk_analytics/`)

| File | Purpose |
| --- | --- |
| `config.py` | Loads `config/platform.yaml`, applies environment overrides, and resolves table names. |
| `spark.py` | Builds the Spark session for local, cluster, or Spark Connect mode with the Iceberg/Nessie/S3 settings. |
| `nessie.py` | Creates and merges run branches and reads reference state. |
| `yaml_executor.py` | The pipeline engine: renders templates, validates structure, loads sources, dispatches steps, writes targets, and returns row counts. |
| `kafka_events.py` | Builds the trigger payload and provides the `AwaitMessageSensor` match function (importable because the triggerer resolves it by import string). |
| `transformations/components.py` | `execute_component()` dispatcher that routes a YAML step to its handler. |
| `transformations/relational.py` | `join` and `lookup` components. |
| `transformations/aggregation.py` | `rollup`, `normalize`, `dedup`, and other shape-changing components. |
| `transformations/shaping.py` | `filter` and `reformat` components. |
| `transformations/expressions.py` | Parses the YAML expression syntax into Spark columns. |
| `transformations/common.py` | Shared helpers and the component error types. |

## Transformation Metadata (`transform/`)

Pipeline logic lives here as data, so a business rule change is a YAML review rather than a code
change. See [Metadata-Driven Architecture](metadata-driven-architecture.md) for the authoring rules.

| Path | Purpose |
| --- | --- |
| `transform/source_to_ods/stage_<entity>_<source>.yaml` | Eight active STAGE pipelines: normalize one source into `nessie.risk_analytics_stage.<entity>_stage_<source>`. |
| `transform/source_to_ods/ods_<entity>_<source>.yaml` | Eight active ODS pipelines: merge a stage table into the standardized `nessie.risk_analytics_ods.<entity>` contract. |
| `transform/source_to_ods/risk_metrics_pipeline_source_to_ods.yaml` | The active risk metrics pipeline; the only place the metric formulas are implemented. |
| `transform/risk_metrics_pipeline.yaml` | Legacy metrics pipeline over the `*_canonical` tables, kept only for the `--data-model legacy` flag. See [Legacy artifacts](#legacy-and-transitional-artifacts). |
| `transform/<entity>_Source<A\|B>_transform.yaml` | Ten legacy source transforms writing the legacy `*_stg` tables. Not referenced by any DAG or script. |

## Interfaces (`ui/`, `api/`)

| File | Purpose |
| --- | --- |
| `ui/business_app.py` | Business dashboard (`:8501`): published metrics, sidebar service health, per-entity Kafka state, and run-state totals. |
| `ui/developer_app.py` | Developer control plane (`:8502`): DAG catalog joined to live Airflow state, per-DAG run inspection and trigger, YAML validate/preview, `/tables`, and pipeline execution. |
| `ui/common.py` | Shared adapters (Spark Connect, Airflow REST, Nessie, catalog metadata) kept out of the presentation code so both UIs behave identically. |
| `api/app.py` | FastAPI operations API (`:8000`): the links portal plus `/health`, `/tables`, `/pipeline/execute`, and the YAML validation endpoints. |

## Docker (`docker/`)

| Path | Purpose |
| --- | --- |
| `docker/spark/Dockerfile` | Spark image used by master, worker, Spark Connect, and the Kafka consumer. |
| `docker/airflow/Dockerfile` | Airflow image with the providers and the project package installed. |
| `docker/ui/Dockerfile` | Shared image for both Streamlit UIs and the FastAPI API. |
| `docker/notebook/Dockerfile` | JupyterLab image with a matching PySpark client. |
| `docker/seaweedfs/s3.json` | S3 bucket and access configuration for the local warehouse. |
| `docker/seaweedfs/iam.json` | Identity/permission configuration for the SeaweedFS S3 API. |

## Operations Scripts (`scripts/`)

Each script carries a header stating why it exists and the steps it performs; the full write-up is in
[Scripts Reference](scripts-reference.md).

| File | Purpose |
| --- | --- |
| `run_risk_analytics_pipeline.ps1` | End-to-end run through Airflow: platform startup, DAG registration preflight, trigger, bounded wait, validation. |
| `run_local_python_no_airflow.py` | The same flow without Airflow, using Docker services and the host virtual environment, plus previews and a markdown run report. |
| `run_manual_pipeline_sequence.ps1` | STAGE, ODS, and metrics jobs only, one Spark session per layer, for iterating on YAML changes. |
| `health_check.py` | Probes every service endpoint (and optionally queries `risk_metrics`) because "container up" is not "service ready". |
| `validate_pipeline_status.py` | Diffs registered DAGs against the shipped 27, warns on legacy metadata and paused DAGs, and validates Nessie and ODS data. |
| `validate_dags.py` | Parses the DAG folder with a real `DagBag` and asserts the expected DAG ids; run this before pushing DAG changes. |

## Tests (`tests/`)

Run with `python -m unittest discover -s tests -p "test_*.py" -t .`; see [Testing](testing.md).

| File | Covers |
| --- | --- |
| `support.py` | Fake Spark/DataFrame doubles so transformation logic is testable without a JVM. |
| `test_yaml_executor.py` | Template rendering, validation errors, source loading, and step dispatch. |
| `test_run_source_to_ods_step.py` | STAGE/ODS argument handling, including the repeatable `--entity` flag. |
| `test_run_risk_pipeline.py` | Branch creation, merge, event publication, and the metrics pipeline selection. |
| `test_bootstrap.py` | DDL coverage and deterministic seeding. |
| `test_config.py` | Configuration defaults and environment overrides. |
| `test_spark_session.py` | Session construction per execution mode. |
| `test_nessie.py` | Reference and merge behavior, including failure fallbacks. |
| `test_kafka_events.py` | Trigger payload shape and the entity match function used by the sensors. |
| `test_airflow_ra_dags.py` | Builds the DAGs and asserts ids, structure, and dependency edges. |
| `test_airflow_dockerfile.py` | Guards the pins the Airflow image depends on. |
| `test_api_triggers.py` | Operations API endpoints and DAG trigger payloads. |
| `test_health_check.py` | Probe results and exit codes. |
| `test_pipeline_script.py` | The PowerShell runner's DAG coverage and ordering, and fails if any file reintroduces a legacy DAG id. |

## Documentation (`docs/`)

| File | Purpose |
| --- | --- |
| `index.md` | Site landing page, audience routing, and documentation map. |
| `architecture.md` | The single architecture reference: system design, tech stack, and all diagrams. |
| `data-model-risk-metrics.md` | Layer contracts, Kafka topic-to-table mapping, lineage, and metric formulas. |
| `platform-interfaces-and-operations.md` | Every interface (UIs, API, Airflow, Dremio, Kafka UI, JupyterLab) with example payloads and commands. |
| `runbooks.md` | Setup, first run, offline run, and the local no-Airflow path. |
| `scripts-reference.md` | Why each `scripts/` entry exists, when to use it, and its steps. |
| `metadata-driven-architecture.md` | How to author and validate YAML pipelines. |
| `production_setup.md` | What to change for a non-local deployment. |
| `testing.md` | Test layout, how to run the suite, and the validation gates. |
| `troubleshooting.md` | Symptom-to-fix table for the failures this platform actually produces. |
| `dependency-cache-guide.md` | Where dependencies are downloaded and cached, and how to inspect the volumes. |
| `project-reference.md` | This page. |
| `readme.md` | Recommended reading order inside the site. |

## Data, Notebooks, and Editor Support

| Path | Purpose |
| --- | --- |
| `data/sourcea/*.json`, `data/sourceb/*` | Small deterministic seed files so a fresh clone produces identical, reviewable metrics. |
| `notebooks/risk_analytics_spark_connect_nessie_queries.ipynb` | Query the published tables through Spark Connect. |
| `notebooks/risk_analytics_operational_checks.ipynb` | Notebook-based service and data checks. |
| `.vscode/tasks.json` | Curated debug tasks: run the pipeline, validate DAGs, tail service logs, query metrics, serve the docs. |
| `.vscode/settings.json` | Python interpreter selection for the workspace. |
| `.github/workflows/ci.yml` | Lint, type-check, unit tests, `docker compose config`, and DAG parsing on every push and PR. |
| `.github/workflows/docs-pages.yml` | Builds and publishes the MkDocs site. |
| `.github/workflows/docs-pr-check.yml` | Builds the docs on pull requests so a broken link fails the PR. |

## Generated and Runtime Paths (Not in Git)

These appear locally but are ignored on purpose; delete them freely.

| Path | Produced by |
| --- | --- |
| `site/` | `mkdocs build`. |
| `.venv/` | `setup_venv.py`. |
| `logs/run-info/` | Markdown run reports from `run_local_python_no_airflow.py`. |
| `postgres-data/`, `seaweed-data/`, `dremio-data/`, `spark-ivy-cache/` | Docker volume mounts for service state and the Spark package cache. |
| `__pycache__/`, `.pytest_cache/` | Python tooling. |

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

The active published path is STAGE -> ODS -> `nessie.risk_analytics_ods.risk_metrics`. A pre-refactor
"legacy" data model still ships alongside it, and it is worth knowing exactly what belongs to it so
you do not debug the wrong pipeline:

| Artifact | State |
| --- | --- |
| `transform/<entity>_Source<A\|B>_transform.yaml` (10 files) | Write the legacy `nessie.risk_analytics.*_stg` tables. No DAG, script, or test runs them. |
| `transform/risk_metrics_pipeline.yaml` | Reads the `*_canonical` tables. Only reachable through `run_risk_pipeline.py --data-model legacy`. |
| `*_canonical` and `*_stg` tables in `nessie.risk_analytics` | Created by `jobs/create_tables.py` but never populated by the active flow, so a legacy run returns zero rows. |
| `data/sourcea/trades.json`, `data/sourcea/trade_product.json` | Seed the legacy `trades` / `trade_product` source tables; the active model uses `deals`. |
| `data/sourceb/product`, `data/sourceb/trans` | Source B inputs for the legacy product/trans transforms. |

`--data-model legacy` is still the default flag value of `jobs/run_risk_pipeline.py`, while every DAG
and script passes `--data-model source-to-ods` explicitly. If you invoke the job by hand, pass the
flag. Removing the legacy model entirely (YAMLs, DDL, seeds, and the flag) is a safe follow-up once
you are sure nothing external reads those tables.

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
