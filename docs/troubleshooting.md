# Troubleshooting

> Quick links: [Overview](index.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md) | [Interfaces](platform-interfaces-and-operations.md)

This page collects practical commands for common development and operations scenarios.

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
docker compose exec airflow-webserver airflow dags list-runs -d risk_analytics_pipeline
```

List task states for a run:

```powershell
docker compose exec airflow-webserver airflow tasks states-for-dag-run risk_analytics_pipeline <dag_run_id>
```

Restart Airflow services:

```powershell
docker compose restart airflow-webserver airflow-scheduler
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
docker compose exec airflow-webserver airflow dags pause risk_analytics_kafka_listener

docker compose start kafka-entity-stream
docker compose exec airflow-webserver airflow dags unpause risk_analytics_kafka_listener
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
