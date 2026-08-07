# Local Development Guide

This guide explains how to run the Risk Analytics platform locally without the full Docker stack, using different execution modes and environments.

## Platform Setup by Environment

### Windows Development

**Option 1: React UI/API (Recommended)**
The React UI and FastAPI backend work directly on Windows without Spark requirements.

```powershell
# Install Python 3.11
py -3.11 --version

# Set up virtual environment
py -3.11 setup_venv.py

# Start the platform
.\scripts\start_local.ps1
```

Access:
- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

**Option 2: WSL (For Full Spark Functionality)**
For running Spark jobs on Windows, use WSL for proper Java and Hadoop support.

```powershell
# Install WSL
wsl --install

# In WSL terminal:
cd /mnt/d/riskanalytics_df
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt -r requirements/notebook.txt -r requirements/docs.txt -r requirements/airflow.txt -r requirements/spark.txt -r requirements/ui.txt
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

**Option 3: Docker (Full Production Stack)**
```powershell
docker-compose up
```

### Linux/Mac Development

**Option 1: Local Mode**
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt -r requirements/notebook.txt -r requirements/docs.txt -r requirements/airflow.txt -r requirements/spark.txt -r requirements/ui.txt
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

**Option 2: Docker Mode**
```bash
docker-compose up
```

## Execution Modes

### 1. Docker Mode (Current Production Setup)
- Full Docker stack with all services
- Cluster Spark
- Remote Nessie catalog and SeaweedFS storage
- Airflow orchestration
- Kafka streaming

**Use when:** Running full production-like environment

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
```

### 2. Hybrid Mode (Recommended for Development)
- Docker Spark services (Spark Connect)
- Remote Nessie catalog and SeaweedFS storage (Docker containers)
- Script-based orchestration (no Airflow)
- Optional Kafka streaming

**Use when:** Developing data pipelines with catalog consistency

```powershell
# Step 1: Start required services
.\scripts\start_hybrid_services.ps1

# Step 2: Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

### 3. Local Mode (Zero External Dependencies)
- Local Spark
- Local file-based catalog
- Local filesystem storage
- Script-based orchestration
- No streaming

**Use when:** Quick testing without external services

```powershell
# Step 1: Set up local environment
.\scripts\setup_local_env.ps1

# Step 2: Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode local -AsOfDate 2026-07-18
```

## Setup Instructions

### Prerequisites

1. **Python 3.11+** (required to match Docker runtime images, WSL Ubuntu 26.04 uses Python 3.14)
2. **Docker Desktop** (for hybrid/docker modes)
3. **PowerShell** (Windows) or bash (Linux/Mac)
4. **Java 17+** (required for local mode only, not for hybrid/docker modes)
5. **WSL** (recommended for Windows Spark jobs)

### Local Mode Setup

```powershell
# Set up Python environment (requires Java)
.\scripts\setup_local_env.ps1

# Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode local -AsOfDate 2026-07-18
```

### Hybrid Mode Setup

```powershell
# Set up Python environment (no Java required)
.\scripts\setup_local_env.ps1

# Start required Docker services (Nessie, SeaweedFS, Spark)
.\scripts\start_hybrid_services.ps1

# Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

To include Kafka:

```powershell
# Start with Kafka
.\scripts\start_hybrid_services.ps1

# Run pipeline (Kafka will be available if needed)
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

## Configuration

Execution modes are configured in `config/modes/`:

- `docker.yaml` - Full Docker stack configuration
- `hybrid.yaml` - Local Spark, remote catalog/storage
- `local.yaml` - Everything local

You can override configuration via environment variables:

```powershell
$env:EXECUTION_MODE = "hybrid"
$env:NESSIE_URI = "http://localhost:19120/api/v2"
$env:S3_ENDPOINT = "http://localhost:8333"
```

## Running Individual Pipeline Steps

```powershell
# Set execution mode
$env:EXECUTION_MODE = "hybrid"

# Bootstrap only
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18 -SkipOrchestration -SkipRiskMetrics

# STAGE/ODS only
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18 -SkipBootstrap -SkipRiskMetrics

# Risk metrics only
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18 -SkipBootstrap -SkipOrchestration
```

## Development Workflow

### 1. Quick Development (Local Mode)
```powershell
# Fast iteration with no external dependencies
.\scripts\run_local_pipeline.ps1 -Mode local -AsOfDate 2026-07-18
```

### 2. Testing with Catalog (Hybrid Mode)
```powershell
# Test with real catalog/storage but local Spark
.\scripts\start_hybrid_services.ps1
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

### 3. Full Testing (Docker Mode)
```powershell
# Full production-like environment
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode offline
```

## Troubleshooting

### Windows PySpark Issues
**Problem:** PySpark fails with "FileNotFoundError" on Windows
**Solution:** Use WSL for Spark jobs, or use the React UI/API directly on Windows

```powershell
# Option 1: Use WSL for Spark jobs
wsl
cd /mnt/d/riskanalytics_df
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18

# Option 2: Use React UI/API on Windows (no Spark required)
.\scripts\start_local.ps1
```

### Python Dependencies
```powershell
# Reinstall dependencies
pip install --upgrade -r requirements/dev.txt
```

### Spark Connection Issues
```powershell
# Check Spark is accessible
python -c "from risk_analytics.spark import create_spark_session; spark = create_spark_session('test'); print(spark.version)"
```

### React UI Issues
```powershell
# Frontend not loading on port 5173
# Kill conflicting processes
Get-NetTCPConnection -LocalPort 5173 -ErrorAction SilentlyContinue | Select-Object OwningProcess
Stop-Process -Id <process_id> -Force

# Rebuild frontend
cd risk-analytics-ui
npm run build
```

### Catalog Connection Issues
```powershell
# Check Nessie (hybrid mode)
Invoke-RestMethod http://localhost:19120/api/v1/config

# Check local catalog (local mode)
# Should create tables in /tmp/iceberg
```

### Storage Issues
```powershell
# Check SeaweedFS (hybrid mode)
Invoke-RestMethod http://localhost:8333

# Check local storage (local mode)
# Check ./data/warehouse directory exists
```

## Performance Considerations

### Local Mode
- **Pros:** Fastest iteration, no external dependencies
- **Cons:** Requires Java, limited scalability, no catalog features, Windows compatibility issues
- **Use for:** Quick testing, development (requires Java or WSL on Windows)

### Hybrid Mode
- **Pros:** Catalog consistency, no Java required, reasonable performance
- **Cons:** Requires Docker services (Spark, Nessie, SeaweedFS)
- **Use for:** Development with catalog features (recommended)

### Docker Mode
- **Pros:** Full feature parity, production-like, cross-platform
- **Cons:** Slower iteration, resource intensive
- **Use for:** Full testing, production deployment

### React UI/API Mode (Windows)
- **Pros:** Works directly on Windows, no Spark required, full UI functionality
- **Cons:** Cannot run Spark jobs directly
- **Use for:** UI development, API testing, platform management on Windows

## Next Steps

After setting up local development:

1. Run your first pipeline in local mode
2. Test data transformations
3. Verify risk metrics calculations
4. Transition to hybrid mode for catalog testing
5. Use Docker mode for full system testing

## WSL Setup for Windows Users

For Windows users who need full Spark functionality, WSL (Windows Subsystem for Linux) is recommended due to PySpark's known compatibility issues with Windows subprocess execution.

### WSL Installation

```powershell
# Install WSL
wsl --install

# Restart computer and complete WSL setup
```

### WSL Development Setup

```bash
# In WSL terminal
cd /mnt/d/riskanalytics_df

# Install Python 3.11
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip

# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements/dev.txt -r requirements/notebook.txt -r requirements/docs.txt -r requirements/airflow.txt -r requirements/spark.txt -r requirements/ui.txt

# Run Spark jobs
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

### WSL vs Windows Development

| Feature | Windows Native | WSL |
|---------|---------------|-----|
| React UI/API | ✅ Full support | ✅ Full support |
| Spark Jobs | ❌ PySpark issues | ✅ Full support |
| Docker | ✅ Full support | ✅ Full support |
| Performance | Good | Better for Spark |
| Setup Complexity | Low | Medium |

### Recommended Windows Workflow

1. **Use Windows native** for:
   - React UI development
   - API development
   - Platform management
   - Quick testing

2. **Use WSL** for:
   - Spark job development
   - Data pipeline testing
   - Full platform testing

3. **Use Docker** for:
   - Production-like testing
   - Full stack validation
   - Deployment testing

## Migration Notes

- Existing Docker scripts continue to work unchanged
- New modes are additive, not replacements
- Configuration system maintains backward compatibility
- Gradual migration recommended
- Windows users can use React UI/API directly without Spark