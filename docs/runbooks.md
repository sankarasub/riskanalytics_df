# Runbook and Local Execution

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Interfaces](platform-interfaces-and-operations.md) | [Scripts Reference](scripts-reference.md) | [Production Setup](production_setup.md) | [Testing](testing.md) | [Troubleshooting](troubleshooting.md)

This is the primary operations guide for setting up and running the platform in all execution modes.

See [Architecture Simplification](architecture_simplification.md) for details on the different execution modes and their architectural differences.

See [Logging and Monitoring](logging_and_monitoring.md) for detailed information about the centralized logging system and Splunk integration.

## Quick Start - Environment Setup

### Step 1: System Requirements

**Required Software:**
- Python 3.11 (required to match Docker runtime)
- Docker Desktop (for Docker/Hybrid modes only)
- Node.js 16+ and npm (for React UI)
- PowerShell (Windows) or Bash (Linux/Mac)
- Java 8+ (for Spark)

**System Resources:**
- **Local Mode**: ~4 GB RAM
- **Hybrid Mode**: ~6 GB RAM
- **Docker Mode**: ~10 GB RAM

### Step 2: Clone and Configure

```powershell
# Clone the repository (if not already done)
git clone <repository-url>
cd riskanalytics_df

# Copy environment configuration
Copy-Item .env.example .env
```

### Step 3: Setup Python Virtual Environment

```powershell
# Create virtual environment and install all dependencies
python setup_venv.py

# This will:
# - Create .venv directory
# - Install all required packages (Spark, UI, notebooks, etc.)
# - Generate requirements-lock.txt for reproducibility
# - Verify local mode dependencies
```

**Expected Output:**
```
Creating virtual environment at .venv
Installing: requirements\ui.txt requirements\notebook.txt requirements\docs.txt requirements\airflow.txt requirements\spark.txt requirements\dev.txt
...
Local-mode dependency check passed: pyspark=4.1.3 pyarrow=18.1.0 grpcio=1.76.0
Wrote resolved dependency lock file: requirements-lock.txt

Virtual environment is ready.
Activate it with: .venv\Scripts\activate
```

### Step 4: Activate Virtual Environment

```powershell
# Windows PowerShell
.venv\Scripts\activate

# Or use Python directly without activation
.venv\Scripts\python.exe <script>
```

### Step 5: Install React UI Dependencies

```powershell
cd risk-analytics-ui
npm install
cd ..
```

### Step 6: Verify Installation

```powershell
# Verify Python packages
.venv\Scripts\python.exe -c "import pyspark, pyarrow, grpc; print('✓ Core dependencies OK')"

# Verify UI build
cd risk-analytics-ui
npm run build
cd ..
```

## Execution Mode Setup

### Local Mode Setup (Fastest Development)

**No external services required - everything runs locally.**

```powershell
# Set execution mode
$env:EXECUTION_MODE = "local"

# Verify configuration
.venv\Scripts\python.exe -c "from risk_analytics.config import load_config; print(load_config())"

# You're ready to run!
# See "Local Mode Runbook" below
```

### Hybrid Mode Setup (Local Spark + Remote Catalog/Storage)

**Requires Nessie and SeaweedFS services.**

```powershell
# Start required Docker services
docker compose up -d nessie seaweedfs

# Wait for services to be healthy (30-60 seconds)
docker compose ps

# Set execution mode
$env:EXECUTION_MODE = "hybrid"

# Verify connectivity
curl http://localhost:19120/api/v2/config  # Nessie
curl http://localhost:8333                  # SeaweedFS

# You're ready to run!
# See "Hybrid Mode Runbook" below
```

### Docker Mode Setup (Full Production Stack)

**Requires all Docker services.**

```powershell
# Start full Docker stack
docker compose up --build -d

# Wait for all services to be healthy (2-3 minutes)
docker compose ps

# Set execution mode
$env:EXECUTION_MODE = "docker"

# Verify Airflow is ready
docker compose exec airflow-webserver airflow dags list

# You're ready to run!
# See "Docker Mode Runbook" below
```

## Execution Modes Overview

The platform supports three execution modes:

| Mode | Complexity | Speed | Services Required | Use Case |
|------|-----------|-------|------------------|----------|
| **Local** | Simplest | Fastest | None | Development, debugging |
| **Hybrid** | Medium | Fast | Nessie, SeaweedFS | Testing with persistence |
| **Docker** | Full | Medium | All services | Production-like, full features |

Choose the mode that fits your current needs:
- **Local mode** for quick development iterations
- **Hybrid mode** for testing with data persistence
- **Docker mode** for production-like environment with all features

---

## Local Mode Runbook

### Architecture
```
Files → Stage Tables → ODS Tables → Risk Metrics
```

### Prerequisites
- ✓ Python 3.11 installed
- ✓ Virtual environment created (`python setup_venv.py`)
- ✓ Node.js and npm installed
- ✓ Execution mode set to `local`

### Step 1: Set Execution Mode

```powershell
$env:EXECUTION_MODE = "local"
```

### Step 2: Start Local Platform

```powershell
# Start the platform in local mode
.\scripts\start_local.ps1
```

**This will:**
- Create local data directory if needed
- Start local embedded Spark
- Launch React UI at http://localhost:5173
- Start FastAPI backend at http://localhost:8000

**Expected Output:**
```
Starting Risk Analytics Platform in LOCAL mode...
✓ Local Spark session created
✓ React UI starting at http://localhost:5173
✓ FastAPI backend starting at http://localhost:8000
```

### Step 3: Create Tables

```powershell
# Open new terminal (keep platform running)
.venv\Scripts\python.exe jobs\bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

**Expected Output:**
```
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | PIPELINE START: bootstrap
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | Execution Mode: local
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | CONNECTION DETAILS:
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Catalog: local
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Catalog Type: memory
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Storage Type: local
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Spark Mode: local
...
STEP COMPLETE: create_source_to_ods_tables
Duration: 1234.56ms (1.23s)
PIPELINE COMPLETED: bootstrap
```

### Step 4: Run Stage Transformations

```powershell
# Stage customer data from SourceA
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18

# Stage asset data
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity asset --source sourcea --as-of-date 2026-07-18

# Stage collateral data
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity collateral --source sourcea --as-of-date 2026-07-18

# Stage deals data
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity deals --source sourcea --as-of-date 2026-07-18
```

**Expected Output:**
```
PIPELINE START: source_to_ods_stage_sourcea
STEP START: configure_file_paths
STEP COMPLETE: configure_file_paths
STEP START: stage_customer_sourcea
Records Processed: 5
Duration: 456.78ms (0.46s)
STEP COMPLETE: stage_customer_sourcea
PIPELINE COMPLETED: source_to_ods_stage_sourcea
```

### Step 5: Run ODS Transformations

```powershell
# ODS customer data
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18

# ODS asset data
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity asset --source sourcea --as-of-date 2026-07-18

# ODS collateral data
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity collateral --source sourcea --as-of-date 2026-07-18

# ODS deals data
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity deals --source sourcea --as-of-date 2026-07-18
```

### Step 6: Run Risk Metrics

```powershell
.venv\Scripts\python.exe jobs\run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

**Expected Output:**
```
PIPELINE START: risk_metrics_pipeline
STEP START: create_nessie_branch
STEP COMPLETE: create_nessie_branch
STEP START: run_risk_pipeline
Records Processed: 5
Duration: 2345.67ms (2.35s)
STEP COMPLETE: run_risk_pipeline
PIPELINE COMPLETED: risk_metrics_pipeline
```

### Step 7: Validate Results

```powershell
# Check health via API
Invoke-RestMethod http://localhost:8000/api/platform/health

# Check available tables
Invoke-RestMethod http://localhost:8000/api/data/tables

# Check risk metrics
Invoke-RestMethod http://localhost:8000/api/metrics/summary?as_of_date=2026-07-18
```

### Step 8: Verify in UI

1. Open http://localhost:5173
2. Navigate to **Dashboard** - verify health status shows all systems healthy
3. Navigate to **Data Explorer** - browse created tables:
   - `risk_analytics_stage.customer_stage_sourcea`
   - `risk_analytics_ods.customer`
   - `risk_analytics_ods.risk_metrics`
4. Navigate to **Risk Metrics** - view calculated metrics

### Step 9: Run Validation Notebook (Recommended)

```powershell
# Set execution mode (if not already set)
$env:EXECUTION_MODE = "local"

# Start Jupyter
.venv\Scripts\jupyter.exe notebook

# Open notebooks/validation_and_testing.ipynb
# Run all cells to validate the complete pipeline
```

**Validation notebook will check:**
- ✓ Environment configuration
- ✓ Table structure
- ✓ Data loading
- ✓ Data quality
- ✓ Business logic
- ✓ Performance
- ✓ End-to-end pipeline

### Step 10: Check Logs

```powershell
# View execution logs
Get-Content logs\pipeline_execution.log -Tail 50

# View structured metrics
Get-Content logs\pipeline_metrics.json
```

### Local Mode Troubleshooting

**Issue:** Platform won't start
```powershell
# Check if port 8000 or 5173 is in use
netstat -ano | findstr :8000
netstat -ano | findstr :5173
```

**Issue:** Spark connection failed
```powershell
# Verify Java is installed
java -version

# Verify Python 3.11
python --version
```

**Issue:** Tables not found
```powershell
# Run bootstrap first
.venv\Scripts\python.exe jobs\bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

---

## Hybrid Mode Runbook

### Architecture
```
Files → Stage Tables → ODS Tables → Risk Metrics
(Remote Nessie catalog + SeaweedFS storage)
```

### Prerequisites
- ✓ All Local Mode prerequisites
- ✓ Docker Desktop running
- ✓ Nessie and SeaweedFS started

### Step 1: Start Required Services

```powershell
# Start only Nessie and SeaweedFS
docker compose up -d nessie seaweedfs

# Wait for services to be healthy (30-60 seconds)
docker compose ps
```

### Step 2: Set Execution Mode

```powershell
$env:EXECUTION_MODE = "hybrid"
```

### Step 3: Verify Services

```powershell
# Check Nessie
curl http://localhost:19120/api/v2/config

# Check SeaweedFS
curl http://localhost:8333
```

### Step 4: Start Hybrid Platform

```powershell
# Start the platform in hybrid mode
.\scripts\start_hybrid.ps1
```

**This will:**
- Verify Nessie and SeaweedFS are running
- Start local Spark with remote catalog connection
- Launch React UI at http://localhost:5173
- Start FastAPI backend at http://localhost:8000

### Step 5: Run Pipeline (Same as Local Mode)

```powershell
# Create tables
.venv\Scripts\python.exe jobs\bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18

# Stage transformations
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity asset --source sourcea --as-of-date 2026-07-18
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity collateral --source sourcea --as-of-date 2026-07-18
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity deals --source sourcea --as-of-date 2026-07-18

# ODS transformations
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity asset --source sourcea --as-of-date 2026-07-18
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity collateral --source sourcea --as-of-date 2026-07-18
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity deals --source sourcea --as-of-date 2026-07-18

# Risk metrics
.venv\Scripts\python.exe jobs\run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

### Step 6: Validate Results

```powershell
# Check health via API
Invoke-RestMethod http://localhost:8000/api/platform/health

# Check Nessie connection
Invoke-WebRequest http://localhost:19120/api/v2/config

# Check SeaweedFS connection
Invoke-WebRequest http://localhost:8333

# Check data
Invoke-RestMethod http://localhost:8000/api/data/tables
Invoke-RestMethod http://localhost:8000/api/metrics/summary?as_of_date=2026-07-18
```

### Step 7: Verify in UI

1. Open http://localhost:5173
2. Navigate to **Dashboard** - verify all services healthy
3. Navigate to **Data Explorer** - browse tables in remote catalog
4. Navigate to **Risk Metrics** - view metrics

### Step 8: Run Validation Notebook

```powershell
# Set execution mode
$env:EXECUTION_MODE = "hybrid"

# Start Jupyter
.venv\Scripts\jupyter.exe notebook

# Open notebooks/validation_and_testing.ipynb
# Run all cells to validate the complete pipeline
```

### Hybrid Mode Troubleshooting

**Issue:** Cannot connect to Nessie
```powershell
# Verify Nessie is running
docker compose ps nessie

# Check Nessie logs
docker compose logs nessie

# Restart Nessie
docker compose restart nessie
```

**Issue:** Catalog connection errors
```powershell
# Check Nessie URI in config
Get-Content config\modes\hybrid.yaml

# Verify Nessie is accessible
curl http://localhost:19120/api/v2/config
```

---

## Docker Mode Runbook

### Architecture
```
Files/Kafka → Source Tables → Stage Tables → ODS Tables → Risk Metrics
```

### Prerequisites
- ✓ All Local Mode prerequisites
- ✓ Docker Desktop running
- ✓ Full Docker stack started

### Step 1: Start Full Docker Stack

```powershell
docker compose up --build -d
```

### Step 2: Verify Services

```powershell
docker compose ps --all
```

### Step 3: Set Execution Mode

```powershell
$env:EXECUTION_MODE = "docker"
```

### Step 4: Run Pipeline via Airflow

```powershell
# Unpause DAGs
foreach ($source in @('sourceA', 'sourceB')) {
  foreach ($entity in @('customer', 'asset', 'collateral', 'deals')) {
    docker compose exec airflow-webserver airflow dags unpause "ra_${source}_${entity}_stage"
    docker compose exec airflow-webserver airflow dags unpause "ra_${source}_${entity}_ods"
  }
}

# Trigger bootstrap DAG
docker compose exec airflow-webserver airflow dags trigger ra_createtables_and_data --conf '{"as_of_date":"2026-07-18"}'
```

### Step 5: Monitor Execution

```powershell
# Check DAG runs
docker compose exec airflow-webserver airflow dags list-runs -d ra_createtables_and_data
docker compose exec airflow-webserver airflow dags list-runs -d ra_stage_to_ods_orchestration
docker compose exec airflow-webserver airflow dags list-runs -d ra_riskmetrics_eval_ods
```

### Step 6: Validate Results

```powershell
# Health check
docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py --check-iceberg

# Data checks
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics"
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT MAX(as_of_date) AS latest_as_of FROM nessie.risk_analytics_ods.risk_metrics"
```

### Step 7: Access UIs

- Business Dashboard: http://localhost:8501
- Developer Control Plane: http://localhost:8502
- Unified React UI: http://localhost:3000
- API: http://localhost:8000

### Docker Mode Troubleshooting

**Issue:** Services not starting
```powershell
# Check Docker Desktop is running
docker version

# Check logs
docker compose logs spark-master
docker compose logs airflow-webserver
```

**Issue:** DAGs not registered
```powershell
# Restart Airflow
docker compose restart airflow-webserver airflow-scheduler

# Re-sync DAGs
docker compose exec airflow-webserver airflow dags list
```

---

## Mode-Specific Validation

### Local Mode Validation
```powershell
# 1. Check local storage directory exists
Test-Path ".\data\warehouse"

# 2. Verify Spark can be created locally
.venv\Scripts\python.exe -c "from risk_analytics.spark import create_spark_session; spark = create_spark_session('test', 'main', 'local'); print('Local Spark OK'); spark.stop()"

# 3. Verify API health
Invoke-RestMethod http://localhost:8000/api/platform/health

# 4. Check tables are in local catalog
Invoke-RestMethod http://localhost:8000/api/data/tables
# Should return tables from 'local' catalog
```

### Hybrid Mode Validation
```powershell
# 1. Verify remote services are running
Invoke-WebRequest http://localhost:19120/api/v2/config
Invoke-WebRequest http://localhost:8333

# 2. Verify Spark can connect to remote catalog
.venv\Scripts\python.exe -c "from risk_analytics.spark import create_spark_session; spark = create_spark_session('test', 'main', 'hybrid'); print('Hybrid Spark OK'); spark.stop()"

# 3. Verify API health
Invoke-RestMethod http://localhost:8000/api/platform/health

# 4. Check tables are in remote catalog
Invoke-RestMethod http://localhost:8000/api/data/tables
# Should return tables from 'nessie' catalog
```

### Docker Mode Validation
```powershell
# 1. Check all services are running
docker compose ps --all

# 2. Health check
docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py --check-iceberg

# 3. Verify data
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics"

# 4. Check DAG status
docker compose exec airflow-webserver airflow dags list
```

---

## Quick Validation Commands

### Service Health
```powershell
# Local/Hybrid
Invoke-RestMethod http://localhost:8000/api/platform/health

# Docker
docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py --check-iceberg
```

### Data Verification
```powershell
# Local/Hybrid (via API)
Invoke-RestMethod http://localhost:8000/api/data/tables
Invoke-RestMethod http://localhost:8000/api/metrics/summary?as_of_date=2026-07-18

# Docker (via Spark SQL)
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics"
```

### UI Validation
```powershell
# Check UI endpoints
Invoke-WebRequest http://localhost:8000/health -UseBasicParsing
Invoke-WebRequest http://localhost:5173 -UseBasicParsing  # React UI
Invoke-WebRequest http://localhost:8501/_stcore/health -UseBasicParsing  # Business UI (Docker)
Invoke-WebRequest http://localhost:8502/_stcore/health -UseBasicParsing  # Developer UI (Docker)
```

---

## Troubleshooting

### Local Mode Issues
- **Spark fails to start**: Ensure Java 8+ is installed
- **Tables not found**: Verify bootstrap ran successfully
- **API connection refused**: Check backend is running on port 8000

### Hybrid Mode Issues
- **Cannot connect to Nessie**: Ensure `docker compose up -d nessie seaweedfs` ran first
- **Catalog connection errors**: Check Nessie URI in config/modes/hybrid.yaml
- **Storage errors**: Verify SeaweedFS is accessible at configured endpoint

### Docker Mode Issues
- **Services not starting**: Check Docker Desktop is running
- **DAGs not registered**: Run `docker compose restart airflow-webserver airflow-scheduler`
- **Memory issues**: Increase Docker Desktop memory allocation

---

## Clean Restart

### Local Mode
```powershell
# Stop services (Ctrl+C in terminal)
# Remove local data if needed
Remove-Item -Recurse -Force .\data\warehouse
# Restart
.\scripts\start_local.ps1
```

### Hybrid Mode
```powershell
# Stop services (Ctrl+C in terminal)
# Stop Docker services
docker compose down
# Restart
docker compose up -d nessie seaweedfs
.\scripts\start_hybrid.ps1
```

### Docker Mode
```powershell
# Stop and remove volumes
docker compose down -v
# Full rebuild
docker compose up --build -d
```

---

## Mode Switching

To switch between execution modes:

1. **Stop all services**
   - Local/Hybrid: Ctrl+C in terminal
   - Docker: `docker compose down`

2. **Set execution mode**
   ```powershell
   $env:EXECUTION_MODE = "local"    # or "hybrid" or "docker"
   ```

3. **Start services for new mode**
   - Local: `.\scripts\start_local.ps1`
   - Hybrid: `docker compose up -d nessie seaweedfs` then `.\scripts\start_hybrid.ps1`
   - Docker: `docker compose up -d`

4. **Verify mode switch**
   - Check React UI Dashboard page for current execution mode
   - Verify API health shows correct mode

---

## Developer Quick Reference

### One-Command Setup

```powershell
# Clone, setup, and start in local mode
git clone <repo-url> && cd riskanalytics_df
Copy-Item .env.example .env
python setup_venv.py
cd risk-analytics-ui; npm install; cd ..
$env:EXECUTION_MODE = "local"
.\scripts\start_local.ps1
```

### Common Commands

```powershell
# Activate venv
.venv\Scripts\activate

# Create tables
.venv\Scripts\python.exe jobs\bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18

# Run stage transformation
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18

# Run ODS transformation
.venv\Scripts\python.exe jobs\run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18

# Run risk metrics
.venv\Scripts\python.exe jobs\run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods

# Run validation notebook
.venv\Scripts\jupyter.exe notebook
```

### Log Files

```powershell
# View execution logs
Get-Content logs\pipeline_execution.log -Tail 50

# View Splunk-compatible logs
Get-Content logs\splunk_pipeline.log -Tail 50

# View structured metrics
Get-Content logs\pipeline_metrics.json
```

For detailed validation procedures, see the [Validation and Testing Guide](validation_guide.md).

For monitoring and logging, see [Logging and Monitoring](logging_and_monitoring.md).