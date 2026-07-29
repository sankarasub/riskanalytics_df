# Runbook and Local Execution

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Interfaces](platform-interfaces-and-operations.md) | [Production Setup](production_setup.md) | [Testing](testing.md) | [Troubleshooting](troubleshooting.md)

This is the primary operations guide for setting up and running the platform.

## Prerequisites

- Docker Desktop with Compose
- PowerShell (Windows)
- Python 3.11+ for local host execution and tests

Initial setup:

```powershell
Copy-Item .env.example .env
```

## Run Modes

### Mode A: Full first build

Use after clone or dependency changes.

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
```

What it does:

- Builds and starts required services
- Creates tables and seeds data
- Runs source-to-ODS stage and ODS flows
- Runs final risk pipeline
- Runs validation checks

### Mode B: Regular offline run

Use for routine development when images and cache are already available.

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode offline
```

### Mode C: Pipeline run on already-running platform

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18
```

If PowerShell blocks script execution with `running scripts is disabled on this system`, run this in the same session and retry:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Airflow-Orchestrated Flow

DAG order:

1. `ra_createtables_and_data` (creates tables, seeds sources, then triggers step 2)
2. `ra_stage_to_ods_orchestration` (triggers `ra_<source>_<entity>_stage` then `ra_<source>_<entity>_ods` per entity)
3. `ra_riskmetrics_eval_ods` (triggered automatically once every ODS load completes)

Unpause the leaf DAGs once so the orchestration can trigger them:

```powershell
foreach ($source in @('sourceA', 'sourceB')) {
  foreach ($entity in @('customer', 'asset', 'collateral', 'deals')) {
    docker compose exec airflow-webserver airflow dags unpause "ra_${source}_${entity}_stage"
    docker compose exec airflow-webserver airflow dags unpause "ra_${source}_${entity}_ods"
  }
}
```

Useful commands:

```powershell
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-runs -d ra_riskmetrics_eval_ods
docker compose exec airflow-webserver airflow dags trigger ra_riskmetrics_eval_ods --conf '{"as_of_date":"2026-07-18"}'
```

## Local Run Without Airflow

Use this when you want Docker-backed services but local Python execution.

### Quick command

```powershell
python .\setup_venv.py
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --docker-mode reuse
```

Upgrade libraries in the local Python environment and refresh the lock snapshot:

```powershell
py -3.11 .\setup_venv.py --update-libraries
```

Useful options:

- `--docker-mode fresh|reuse|none`
- `--source-mode sourcea|sourceb|both`
- `--run-info-dir <path>`
- `--skip-run-info`

### Minimal manual path (no DAG trigger)

```powershell
$Py = ".\\.venv\\Scripts\\python.exe"
$env:SPARK_REMOTE = "sc://localhost:15002"
$env:NESSIE_URI = "http://localhost:19120/api/v2"

& $Py .\jobs\bootstrap.py --action all --as-of-date 2026-07-18
& $Py .\jobs\run_risk_pipeline.py --as-of-date 2026-07-18 --run-id local-manual-20260718 --data-model source-to-ods
```

## Run Verification

### Service health

```powershell
docker compose ps --all
docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py --check-iceberg
```

### Data checks

```powershell
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics"
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT MAX(as_of_date) AS latest_as_of FROM nessie.risk_analytics_ods.risk_metrics"
```

### UI endpoint checks

```powershell
Invoke-WebRequest http://localhost:8000/health -UseBasicParsing
Invoke-WebRequest http://localhost:8501/_stcore/health -UseBasicParsing
Invoke-WebRequest http://localhost:8502/_stcore/health -UseBasicParsing
```

## Kafka-Triggered Flow (Optional)

Continuous services:

- `kafka-entity-stream` service
- `airflow-triggerer` service, required by the deferrable Kafka sensors
- `ra_kafka_<entity>_stage` DAGs (sensors) for `customer`, `asset`, `collateral`, and `deals`, each triggering `ra_kafka_<entity>_ods` and then `ra_riskmetrics_eval_ods`

Disable continuous mode:

```powershell
docker compose stop kafka-entity-stream
foreach ($entity in "customer", "asset", "collateral", "deals") {
  docker compose exec airflow-webserver airflow dags pause "ra_kafka_${entity}_stage"
}
```

Enable continuous mode:

```powershell
docker compose start kafka-entity-stream
foreach ($entity in "customer", "asset", "collateral", "deals") {
  docker compose exec airflow-webserver airflow dags unpause "ra_kafka_${entity}_stage"
}
```

### Kafka Run Commands

Start only Kafka ingest path components:

```powershell
docker compose up -d kafka kafka-init kafka-ui kafka-entity-stream airflow-webserver airflow-scheduler airflow-triggerer
```

Verify topics:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Publish one customer event from CLI:

```powershell
docker compose exec kafka /bin/sh -c "printf '{\"customer_id\":\"CP001\",\"customer_name\":\"Alpha Capital\",\"legal_entity_id\":\"LE-CP001\",\"rating\":\"A\",\"country_code\":\"US\",\"entity_type\":\"BANK\",\"active_flag\":true,\"as_of_date\":\"2026-07-18\"}\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic risk.customer.ingest"
```

Publish one deals event from Python:

```python
from confluent_kafka import Producer
import json

p = Producer({"bootstrap.servers": "localhost:29092"})
p.produce("risk.deals.ingest", value=json.dumps({
	"deal_id": "D001",
	"trade_id": "T001",
	"customer_id": "CP001",
	"asset_id": "A100",
	"collateral_id": "C100",
	"netting_set_id": "NS-APEX",
	"product_type": "SWAP",
	"trade_date": "2026-07-18",
	"maturity_date": "2031-07-18",
	"currency": "USD",
	"notional": 10000000.0,
	"mark_to_market": 600000.0,
	"status": "ACTIVE",
	"as_of_date": "2026-07-18",
	"volatility": 0.2,
	"fixed_rate": 0.035,
	"strike": 0.0,
	"option_type": "NA"
}).encode("utf-8"))
p.flush(5)
```

Verify DAG chain execution:

```powershell
docker compose exec airflow-webserver airflow dags list-runs -d ra_kafka_customer_stage
docker compose exec airflow-webserver airflow dags list-runs -d ra_kafka_customer_ods
docker compose exec airflow-webserver airflow dags list-runs -d ra_riskmetrics_eval_ods
```

Replace `customer` with `asset`, `collateral`, or `deals` to follow the other entity chains.

Check the shared Spark pool if stage/ODS tasks stay queued:

```powershell
docker compose exec airflow-webserver airflow pools list
```

## Clean Restart Options

Preserve named volumes:

```powershell
docker compose down
docker compose up -d --no-build
```

Remove all local state:

```powershell
docker compose down -v
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
```

## Run Report Artifacts (No-Airflow runner)

Default output:

- `logs/run-info/local_no_airflow_run_YYYYMMDD_HHMMSS.md`

The report captures run timestamp, input parameters, run id, and table row-count checks.

## Testing

Use the dedicated [Testing](testing.md) page for full coverage details.

Quick test commands:

```powershell
python -m pip install -r requirements\ui.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py"
```

Recommended when to run:

- After changing risk logic, YAML execution, or configuration parsing.
- Before rebuilding UI/API images for demo use.
- Before sharing a branch for review.
