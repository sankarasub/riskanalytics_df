# Scripts Reference

Every script in `scripts/` exists to remove a specific class of manual error. This page explains why
each one is there, when to reach for it, and what it does step by step. All are current against the
27 `ra_*` DAGs.

## Which script do I want?

| Goal | Script |
| --- | --- |
| Run everything through Airflow, then validate | `run_risk_analytics_pipeline.ps1` |
| Run everything without Airflow, with previews and a run report | `run_local_python_no_airflow.py` |
| Re-run only the STAGE/ODS/metrics jobs while debugging a YAML change | `run_manual_pipeline_sequence.ps1` |
| Ask "are the services reachable?" | `health_check.py` |
| Ask "is the platform loaded and configured correctly?" | `validate_pipeline_status.py` |
| Ask "do the DAGs parse?" (no platform needed) | `validate_dags.py` |

## `run_risk_analytics_pipeline.ps1`

The single supported end-to-end entry point on Windows. Doing this by hand means starting Compose,
checking readiness, unpausing 27 DAGs, triggering the bootstrap, watching three DAG runs, and only
then validating - and getting that order wrong produces misleading failures, such as querying
`nessie.risk_analytics_ods.risk_metrics` before `ra_riskmetrics_eval_ods` has run.

Steps:

1. Verify the Docker daemon answers; start the platform if Spark is not already running
   (`-PlatformMode first-build|offline|none`; `offline` refuses to build or pull and instead reports
   which cached images are missing).
2. Wait until `spark-master`, `airflow-webserver`, `airflow-scheduler`, `business-ui`, `postgres`,
   and `nessie` report running.
3. Verify Airflow registered all 27 expected `ra_*` DAGs, retrying to absorb scheduler parse delay.
   A stale checkout fails here with remediation rather than triggering a DAG that no longer exists.
4. Report - or with `-RemoveLegacyDags` delete - `risk_analytics_*` DAG metadata left from the
   pre-refactor layout.
5. Unpause every expected DAG over the REST API (including the eight Kafka DAGs).
6. Trigger `ra_createtables_and_data` with `-AsOfDate`.
7. Wait for `ra_createtables_and_data`, `ra_stage_to_ods_orchestration`, and
   `ra_riskmetrics_eval_ods` to succeed, bounded by `-WaitTimeoutMinutes` (default 60).
   `-SkipPipelineWait` restores fire-and-forget behaviour.
8. Run `health_check.py --check-iceberg` as the validation query; `-OpenEndpoints` also opens the
   platform URLs.

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build -RemoveLegacyDags
```

## `run_local_python_no_airflow.py`

The Airflow DAGs are the production entry point, but iterating through the scheduler is slow: DAG
parsing, pool slots, and container logs for every attempt. This script runs the same job entrypoints
the DAGs call (`jobs/bootstrap.py`, `jobs/run_source_to_ods_step.py`, `jobs/run_risk_pipeline.py`) as
subprocesses against Spark Connect, so a full bootstrap-to-metrics run is reproducible from one
command with a debugger attached.

Steps:

1. Optionally start Docker (`--docker-mode fresh|reuse|none`); Spark Connect, Nessie, and SeaweedFS
   still have to run somewhere.
2. Bootstrap: create every table and seed source data.
3. STAGE then ODS for the selected `--source-mode` (`sourcea`, `sourceb`, `both`). All four entities
   go to one process per layer, so each layer costs a single Spark session.
4. Print row counts and a top-5 preview per ODS table.
5. Run the risk pipeline (`--data-model source-to-ods`) with a generated run id.
6. Print the `risk_metrics` count and top 5 rows for the as-of date.
7. Write a markdown run report under `--run-info-dir` (`--skip-run-info` opts out).

```powershell
.\.venv\Scripts\python.exe scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --source-mode both
```

## `run_manual_pipeline_sequence.ps1`

The smallest reproduction of the Airflow fan-out. When a YAML transformation misbehaves you rarely
want Docker lifecycle handling, bootstrap, or run reports - only the jobs, in order, in your own
terminal. It assumes the platform is running and the tables exist.

Steps:

1. For each source in `-Sources` (default both), run the STAGE layer for all four entities in one
   `jobs/run_source_to_ods_step.py` process, then the ODS layer in a second process. Source B input
   paths are container paths, because the Spark Connect server resolves the globs.
2. Run `jobs/run_risk_pipeline.py --data-model source-to-ods` to publish `risk_metrics`.

Any failing step exits immediately with that step's exit code.

```powershell
.\scripts\run_manual_pipeline_sequence.ps1 -AsOfDate 2026-07-18 -Sources sourcea -PythonExecutable .\.venv\Scripts\python.exe
```

## `health_check.py`

Compose reports whether containers are *up*, not whether the services inside them answer. This
script probes every endpoint a developer or DAG depends on, so a failed pipeline can be attributed
to a specific service. It is what the PowerShell runner uses as its final validation step.

Steps:

1. HTTP probes: Nessie REST, SeaweedFS S3 and master, Spark master/worker UIs, Airflow, JupyterLab,
   both Streamlit UIs, the operations API, Dremio, and Kafka UI.
2. TCP probes: Spark master RPC, Spark Connect gRPC, Postgres.
3. Optional `--check-iceberg`: a real Spark Connect query against `nessie.risk_analytics_ods` proving
   the catalog and `risk_metrics` are readable. This only passes after metrics have been published.
4. Print a table; exit non-zero only when a *required* check fails, so optional checks degrade to
   WARN and the script stays usable mid-bootstrap.

```powershell
docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py --host host.docker.internal --postgres-host postgres --check-iceberg
```

## `validate_pipeline_status.py`

Where `health_check.py` answers "are the services reachable?", this answers "is the platform
configured and loaded correctly?". Everything runs from the host over REST and Spark Connect, so no
container exec access is needed.

Steps:

1. Airflow: authenticate against the REST API (trying the local credential candidates) and compare
   the registered inventory with the 27 shipped `ra_*` DAGs. Missing DAGs mean the mounted
   `airflow/dags` folder is stale; registered `risk_analytics_*` DAGs mean pre-refactor metadata rows
   survived; paused expected DAGs are reported as warnings.
2. Nessie: confirm the `main` branch is readable over the v2 REST API.
3. Spark: open a Spark Connect session, list `nessie.risk_analytics_ods`, and count `risk_metrics`.
   Both data steps warn rather than fail, because they depend on a pipeline run having happened.

Exit code is 0 only when Airflow, Nessie, and Spark all pass.

```powershell
.\.venv\Scripts\python.exe scripts\validate_pipeline_status.py
```

## `validate_dags.py`

Runs the same `DagBag` parse the scheduler performs and asserts the expected DAG ids exist, so import
errors are caught in CI instead of on a running platform. It needs Airflow installed but no platform.

```powershell
$env:PYTHONPATH = (Get-Location).Path
.\.venv\Scripts\python.exe scripts\validate_dags.py
```
