# Troubleshooting

> Quick links: [Overview](index.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md) | [Interfaces](platform-interfaces-and-operations.md)

This page collects practical commands for common development and operations scenarios.

## Windows-Specific Issues

### PySpark FileNotFoundError on Windows

**Problem:**
```
FileNotFoundError: [WinError 2] The system cannot find the file specified
```

**Cause:**
PySpark has known compatibility issues with Windows subprocess execution and Hadoop library dependencies.

**Solutions:**
1. **Use WSL for Spark jobs** (recommended)
2. **Use Docker for full platform**
3. **Use React UI/API mode** for Windows development (no Spark required)

See [Windows Setup Guide](windows_setup.md) for detailed instructions.

### Port Conflicts on Windows

**Problem:**
Frontend or backend won't start due to port conflicts.

**Solution:**
```powershell
# Find processes using port 5173 or 8000
Get-NetTCPConnection -LocalPort 5173 -ErrorAction SilentlyContinue | Select-Object OwningProcess

# Kill conflicting processes
Stop-Process -Id <process_id> -Force
```

### React UI Blank Page on Windows

**Problem:**
React UI loads but shows blank page with Material-UI errors.

**Solution:**
1. Check browser console for JavaScript errors (F12)
2. Clear browser cache and hard refresh (Ctrl+Shift+R)
3. Rebuild the frontend:

```powershell
cd risk-analytics-ui
npm run build
```

### Module Import Errors on Windows

**Problem:**
```
ModuleNotFoundError: No module named 'risk_analytics'
```

**Solution:**
All job scripts now automatically add the project root to Python path. If you still encounter this:

```powershell
# Ensure you're in the project directory
cd D:\riskanalytics_df

# Use the virtual environment Python
.venv\Scripts\python.exe jobs\bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

### Python Version Issues on Windows

**Problem:**
```
Python 3.11 is required to match the Docker runtime images; found 3.13.2
```

**Solution:**
```powershell
# Install Python 3.11
py -3.11 --version

# If not available, install it
winget install Python.Python.3.11

# Then run setup script
py -3.11 setup_venv.py
```

## General Issues

### Dependency Conflicts

**Problem:**
```
ERROR: Cannot install protobuf==6.31.1 and protobuf==6.33.0
```

**Solution:**
The protobuf version conflict has been fixed in the requirements files. Ensure you have the latest versions:

```powershell
cd D:\riskanalytics_df
git pull
py -3.11 setup_venv.py
```

## Getting Help

If you encounter issues not covered here:

1. Check the [Windows Setup Guide](windows_setup.md) for Windows-specific solutions
2. Review the [Local Development Guide](local_development.md) for mode-specific issues
3. Check the [Architecture documentation](architecture.md) for system design context
4. Review the [Runbooks](runbooks.md) for step-by-step procedures
5. Open an issue on GitHub with:
   - Your environment (Windows/Linux/Mac, WSL/Docker)
   - Execution mode (local/hybrid/docker)
   - Full error message
   - Steps to reproduce

## Quick Platform State

```powershell
docker compose ps --all
docker compose logs --tail 120 airflow-webserver
docker compose logs --tail 120 spark-master
docker compose logs --tail 120 business-ui
docker compose logs --tail 120 developer-ui
```

## Docker Lifecycle Commands

List containers:

```powershell
docker compose ps --all
```

Start all services:

```powershell
docker compose up -d
```

Build and start:

```powershell
docker compose up --build -d
```

Stop and remove containers/network:

```powershell
docker compose down
```

Stop and remove containers with volumes:

```powershell
docker compose down -v
```

Clean unused Docker data (careful):

```powershell
docker system prune -af
docker volume prune -f
```

## Common Python Run Commands

With Airflow orchestration helper:

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode offline
```

Without Airflow (local Python runner):

```powershell
python .\setup_venv.py
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --docker-mode reuse
```

Update local Python libraries and regenerate the dependency lock:

```powershell
py -3.11 .\setup_venv.py --update-libraries
```

Direct job execution:

```powershell
.\.venv\Scripts\python.exe .\jobs\bootstrap.py --action all --as-of-date 2026-07-18
.\.venv\Scripts\python.exe .\jobs\run_risk_pipeline.py --as-of-date 2026-07-18 --run-id manual-20260718 --data-model source-to-ods
```

## PowerShell Script Execution Blocked

Symptom: script execution policy error.

Fix for current shell only:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Airflow Troubleshooting

List DAGs and runs:

```powershell
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-runs -d ra_riskmetrics_eval_ods
```

List task states for a run:

```powershell
docker compose exec airflow-webserver airflow tasks states-for-dag-run ra_riskmetrics_eval_ods <dag_run_id>
```

Restart Airflow services:

```powershell
docker compose restart airflow-webserver airflow-scheduler airflow-triggerer
```

### Tasks stay in `deferred` forever

The Kafka sensors and the orchestration waits are deferrable, so they need the triggerer:

```powershell
docker compose ps airflow-triggerer
docker compose logs --tail 100 airflow-triggerer
docker compose up -d airflow-triggerer
```

### Stage/ODS tasks stay `queued`

Every spark-submit task takes a slot in the `spark_submit` pool, which defaults to 2 slots so the
Airflow container is not flooded with local Spark drivers. Queued tasks are expected during the
fan-out; raise the cap only if the host has the memory:

```powershell
docker compose exec airflow-webserver airflow pools list
$env:SPARK_SUBMIT_POOL_SLOTS = "4"
docker compose up -d airflow-init
```

### Airflow shows pre-refactor `risk_analytics_*` DAGs

The repository only ships the 27 `ra_*` DAGs. Older names such as
`risk_analytics_create_tables_and_load_data` are metadata rows left in the Airflow database from a
previous revision; they have no DAG file and cannot run. Two things cause them to reappear:

1. The checkout mounted into the containers is stale. Run `git pull`, then `docker compose up -d`
   and confirm `docker compose ps airflow-triggerer` exists (added with the deferrable waits).
2. The metadata rows survived the DAG file removal. Delete them:

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -RemoveLegacyDags
# or, per DAG:
docker compose exec airflow-webserver airflow dags delete -y risk_analytics_create_tables_and_load_data
```

The helper script refuses to trigger anything until every expected `ra_*` DAG is registered, so a
stale checkout fails fast instead of triggering a DAG that no longer exists.

### `TABLE_OR_VIEW_NOT_FOUND: nessie.risk_analytics_ods.risk_metrics`

The validation query ran before the pipeline finished. `ra_createtables_and_data` only fires
`ra_stage_to_ods_orchestration`, which fires `ra_riskmetrics_eval_ods`, so the ODS tables appear
minutes after the trigger call returns. The helper script waits for all three runs (bounded by
`-WaitTimeoutMinutes`, default 60); if you passed `-SkipPipelineWait`, re-run the check afterwards:

```powershell
docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py --host host.docker.internal --postgres-host postgres --check-iceberg
```

### Validate DAG parsing without the platform

```powershell
$env:PYTHONPATH = (Get-Location).Path
.\.venv\Scripts\python.exe scripts\validate_dags.py
```

## Spark and Data Troubleshooting

Check Spark web UI:

- `http://localhost:8080`

Table checks:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SHOW TABLES IN nessie.risk_analytics_ods"
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics"
```

If no rows in `risk_metrics`, rerun source-to-ODS plus risk pipeline.

## UI and API Endpoint Checks

```powershell
Invoke-WebRequest http://localhost:8000/health -UseBasicParsing
Invoke-WebRequest http://localhost:8501/_stcore/health -UseBasicParsing
Invoke-WebRequest http://localhost:8502/_stcore/health -UseBasicParsing
Invoke-WebRequest http://localhost:8088/health -UseBasicParsing
```

Rebuild UI/API services after code changes:

```powershell
docker compose up -d --build business-ui developer-ui links-api
```

### `grpcio >= 1.48.1 must be installed`

The UIs and the notebook talk to Spark through Spark Connect (`SPARK_REMOTE`), which needs the
`connect` extra of PySpark (`grpcio`, `grpcio-status`, `googleapis-common-protos`, `protobuf`,
`pyarrow`). `requirements/ui.txt` and `requirements/notebook.txt` therefore pin
`pyspark[connect]==4.1.3`. A `PySparkImportError: [PACKAGE_NOT_INSTALLED] grpcio ...` means the
container still runs an image built from the older plain `pyspark` pin, so rebuild it:

```powershell
docker compose build --no-cache business-ui developer-ui links-api notebook
docker compose up -d business-ui developer-ui links-api notebook
docker compose exec business-ui python -c "import grpc, pyspark; print(grpc.__version__, pyspark.__version__)"
```

## Kafka Troubleshooting

Check Kafka topics:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Check Kafka UI:

- `http://localhost:8090`

Pause/resume listener flow:

```powershell
docker compose stop kafka-entity-stream
docker compose exec airflow-webserver airflow dags pause ra_kafka_customer_stage  # repeat for asset, collateral, deals

docker compose start kafka-entity-stream
docker compose exec airflow-webserver airflow dags unpause ra_kafka_customer_stage  # repeat for asset, collateral, deals
```

## Cache and Dependency Troubleshooting

Inspect Spark cache volume:

```powershell
docker volume inspect risk-analytics-lakehouse_spark-ivy-cache
docker compose exec spark-master sh -lc "ls -lah /opt/spark/.ivy2"
```

If dependency downloads repeat every run:

- Use `-PlatformMode offline` for steady-state runs.
- Avoid `docker compose down -v` unless reset is intentional.

## Reset Paths

Soft reset (keep data):

```powershell
docker compose down
docker compose up -d --no-build
```

Hard reset (remove local state):

```powershell
docker compose down -v
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
```
