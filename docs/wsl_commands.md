# WSL Windows Commands

This guide provides WSL-specific commands for running the Risk Analytics platform in all execution modes from a Windows Subsystem for Linux environment.

## WSL Setup and Configuration

### Initial WSL Setup

```bash
# Update system packages
sudo apt update
sudo apt install -y python3 python3-venv python3-pip python3-dev

# Navigate to project directory
cd /mnt/d/riskanalytics_df

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip
```

### Install All Dependencies

```bash
# Install all required packages
pip install -r requirements/dev.txt \
              -r requirements/notebook.txt \
              -r requirements/docs.txt \
              -r requirements/airflow.txt \
              -r requirements/spark.txt \
              -r requirements/ui.txt
```

## Mode 1: Local Mode (Zero External Dependencies)

Run everything locally in WSL without Docker - no external services required.

### Quick Start

```bash
cd /mnt/d/riskanalytics_df
source .venv/bin/activate

# Set execution mode
export EXECUTION_MODE=local

# Run bootstrap
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

### Run Individual Pipeline Steps

```bash
# Set execution mode
export EXECUTION_MODE=local

# Bootstrap only
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18

# Source-to-ODS STAGE layer
python jobs/run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18

# Source-to-ODS ODS layer
python jobs/run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18

# Risk metrics calculation
python jobs/run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

### Complete Local Mode Workflow

```bash
cd /mnt/d/riskanalytics_df
source .venv/bin/activate
export EXECUTION_MODE=local

# Step 1: Create tables and seed data
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18

# Step 2: Run STAGE layer for all entities
python jobs/run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer stage --entity asset --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer stage --entity collateral --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer stage --entity deals --source sourcea --as-of-date 2026-07-18

# Step 3: Run ODS layer for all entities
python jobs/run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer ods --entity asset --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer ods --entity collateral --sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer ods --entity deals --sourcea --as-of-date 2026-07-18

# Step 4: Calculate risk metrics
python jobs/run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

### Verification

```bash
# Test Spark session
python -c "from risk_analytics.spark import create_spark_session; spark = create_spark_session('test'); print('Spark version:', spark.version); spark.stop()"

# Check local data directory
ls -la /tmp/iceberg/
```

## Mode 2: Hybrid Mode (Local Spark + Remote Services)

Use local Spark but connect to remote catalog/storage services running in Docker.

### Start Docker Services (from Windows PowerShell)

```powershell
# From Windows PowerShell in project directory
docker-compose up -d nessie seaweedfs spark-master spark-worker
```

### WSL Hybrid Mode Commands

```bash
cd /mnt/d/riskanalytics_df
source .venv/bin/activate

# Set execution mode
export EXECUTION_MODE=hybrid

# Connect to Docker Spark Connect
export SPARK_REMOTE=sc://localhost:15002

# Run pipeline
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

### Complete Hybrid Mode Workflow

```bash
cd /mnt/d/riskanalytics_df
source .venv/bin/activate
export EXECUTION_MODE=hybrid
export SPARK_REMOTE=sc://localhost:15002

# Run complete pipeline
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

### Service Status Checks

```bash
# Check Nessie catalog
curl http://localhost:19120/api/v1/config

# Check SeaweedFS storage
curl http://localhost:8333

# Check Spark Connect
curl http://localhost:15002
```

## Mode 3: Docker Mode (Full Production Stack)

Run complete platform in Docker containers, controlled from WSL.

### Start Docker Services (from Windows PowerShell)

```powershell
# From Windows PowerShell in project directory
docker-compose up -d
```

### WSL Docker Mode Commands

```bash
cd /mnt/d/riskanalytics_df

# Use Docker exec to run commands in containers
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags trigger-ra_createtables_and_data -e '{"as_of_date": "2026-07-18"}'

# Trigger specific DAGs
docker compose exec airflow-webserver airflow dags trigger-ra_customer_stage_sourcea
docker compose exec airflow-webserver airflow dags trigger-ra_customer_ods_sourcea
docker compose exec airflow-webserver airflow dags trigger-ra_riskmetrics_eval_ods
```

### Full Docker Workflow

```powershell
# From Windows PowerShell
docker-compose up -d

# Trigger bootstrap
docker compose exec airflow-webserver airflow dags trigger-ra_createtables_and_data -e '{"as_of_date": "2026-07-18"}'

# Wait for completion, then trigger orchestration
docker compose exec airflow-webserver airflow dags trigger-ra_stage_to_ods_orchestration -e '{"as_of_date": "2026-07-18"}'

# Check DAG runs
docker compose exec airflow-webserver airflow dags list-runs -d ra_createtables_and_data
docker compose exec airflow-webserver airflow dags list-runs -d ra_stage_to_ods_orchestration
```

## Mode 4: React UI/API Development

Run the React UI and FastAPI backend directly from WSL.

### Start Frontend and Backend

```bash
cd /mnt/d/riskanalytics_df
source .venv/bin/activate

# Start FastAPI backend
.venv/bin/python -m uvicorn api.backend:app --reload --port 8000 &

# In another terminal, start React frontend
cd risk-analytics-ui
npm run dev:local
```

### WSL-Specific Port Access

```bash
# Access services from Windows browser
# Frontend: http://localhost:5173
# Backend API: http://localhost:8000
# API Docs: http://localhost:8000/docs

# From WSL, use localhost directly
curl http://localhost:8000/api/platform/health
```

## Testing and Verification

### Spark Session Test

```bash
cd /mnt/d/riskanalytics_df
source .venv/bin/activate

# Local mode test
export EXECUTION_MODE=local
python -c "from risk_analytics.spark import create_spark_session; spark = create_spark_session('test'); print('Spark version:', spark.version); spark.stop()"

# Hybrid mode test
export EXECUTION_MODE=hybrid
export SPARK_REMOTE=sc://localhost:15002
python -c "from risk_analytics.spark import create_spark_session; spark = create_spark_session('test'); print('Spark version:', spark.version); spark.stop()"
```

### Health Checks

```bash
# Check local catalog (local mode)
export EXECUTION_MODE=local
python -c "from risk_analytics.config import load_config; print(load_config())"

# Check Nessie catalog (hybrid mode)
curl http://localhost:19120/api/v1/config

# Check API health
curl http://localhost:8000/api/platform/health
```

## Windows File Access from WSL

### Access Windows Files

```bash
# Windows D: drive is mounted at /mnt/d/
cd /mnt/d/riskanalytics_df

# Edit files in Windows, use them in WSL
# Your Windows edits are immediately available in WSL
```

### Performance Considerations

```bash
# For better I/O performance, consider working in WSL filesystem
# instead of /mnt/d/ when doing heavy file operations
# Copy project to WSL home for better performance:

cp -r /mnt/d/riskanalytics_df ~/riskanalytics_df
cd ~/riskanalytics_df
```

## Environment Variables

### Setting Environment Variables

```bash
# Local mode
export EXECUTION_MODE=local

# Hybrid mode
export EXECUTION_MODE=local
export SPARK_REMOTE=sc://localhost:15002

# Docker mode (uses default config)
export EXECUTION_MODE=docker

# Catalog URI (hybrid mode)
export NESSIE_URI=http://localhost:19120/api/v2

# Storage endpoint (hybrid mode)
export S3_ENDPOINT=http://localhost:8333
```

### Persistent Environment Setup

```bash
# Add to ~/.bashrc for persistence
echo 'export EXECUTION_MODE=local' >> ~/.bashrc
echo 'cd /mnt/d/riskanalytics_df' >> ~/.bashrc
echo 'source .venv/bin/activate' >> ~/.bashrc

# Reload bashrc
source ~/.bashrc
```

## Troubleshooting WSL Issues

### WSL File System Issues

```bash
# If Windows files are not accessible
sudo umount /mnt/d
sudo mount -t drvfs D: /mnt/d
```

### Network Issues

```bash
# Check if Windows services are accessible from WSL
curl http://localhost:19120/api/v1/config
curl http://localhost:8333
curl http://localhost:8000
```

### Python Path Issues

```bash
# Ensure you're in the correct directory
cd /mnt/d/riskanalytics_df

# Verify virtual environment is activated
which python
# Should show: /mnt/d/riskanalytics_df/.venv/bin/python

# If not, activate it
source .venv/bin/activate
```

### Dependency Installation Issues

```bash
# Clear pip cache and reinstall
pip cache purge
pip install --upgrade -r requirements/dev.txt -r requirements/notebook.txt -r requirements/docs.txt -r requirements/airflow.txt -r requirements/spark.txt -r requirements/ui.txt
```

## Performance Optimization

### Use WSL Native Filesystem

```bash
# For better performance, copy project to WSL home
cp -r /mnt/d/rankanalytics_df ~/riskanalytics_df
cd ~/riskanalytics_df

# Set up environment there
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt -r requirements/notebook.txt -r requirements/docs.txt -r requirements/airflow.txt -r requirements/spark.txt -r requirements/ui.txt
```

### Resource Allocation

```bash
# Check available memory
free -h

# Check CPU cores
nproc

# Configure Spark for your resources
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
```

## Integration with Windows

### Running Scripts from Windows PowerShell

```powershell
# You can trigger WSL commands from Windows PowerShell
wsl bash -c "cd /mnt/d/riskanalytics_df && source .venv/bin/activate && python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18"
```

### Windows-WSL File Sharing

```bash
# Access WSL files from Windows
# Windows Explorer: \\wsl$\Ubuntu\home\user\projects
# Or: \\wsl$\Ubuntu\mnt\d\riskanalytics_df
```

## Common WSL Workflows

### Development Workflow

```bash
# 1. Edit files in Windows (IDE in Windows)
# 2. Run commands in WSL (terminal in WSL)
# 3. Test in Windows browser (localhost access works)
```

### Deployment Workflow

```bash
# 1. Develop in WSL (full Spark functionality)
# 2. Test with local mode
# 3. Validate with hybrid mode (requires Docker services on Windows)
# 4. Deploy with Docker mode
```

### Debugging Workflow

```bash
# 1. Use local mode for fast iteration
# 2. Add logging where needed
# 3. Test in WSL to see logs
# 4. Check results in Windows browser
```

## Summary

### WSL Advantages
- ✅ Full PySpark functionality (no Windows subprocess issues)
- ✅ Complete Linux environment
- ✅ Access to Windows files via `/mnt/d/`
- ✅ Better performance for Spark jobs
- ✅ Full platform compatibility

### Recommended WSL Usage
- **Spark Jobs**: Run in WSL for full functionality
- **UI Development**: Can run in WSL or Windows
- **API Development**: Can run in WSL or Windows
- **Docker Services**: Start from Windows, manage from WSL
- **Testing**: Use WSL for comprehensive testing

### Quick Reference

```bash
# Local mode (WSL)
cd /mnt/d/riskanalytics_df && source .venv/bin/activate && export EXECUTION_MODE=local && python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18

# Hybrid mode (WSSL + Docker on Windows)
cd /mnt/d/riskanalytics_df && source .venv/bin/activate && export EXECUTION_MODE=hybrid && export SPARK_REMOTE=sc://localhost:15002 && python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18

# React UI/API (WSSL)
cd /mnt/d/riskanalytics_df && source .venv/bin/activate && .venv/bin/python -m uvicorn api.backend:app --reload --port 8000
cd /mnt/d/riskanalytics_df/risk-analytics-ui && npm run dev:local
```