# Validation and Testing Guide

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Architecture Simplification](architecture_simplification.md) | [Runbooks](runbooks.md) | [Logging and Monitoring](logging_and_monitoring.md)

This guide explains how to use the validation and testing notebook to verify the Risk Analytics platform is working correctly in all execution modes.

## Overview

The `notebooks/validation_and_testing.ipynb` notebook provides comprehensive validation and testing for all execution modes:
- **Local Mode**: Fastest development, no external services
- **Hybrid Mode**: Local Spark + remote catalog/storage  
- **Docker Mode**: Full stack with all services

## Prerequisites

### 1. Set Execution Mode

Before starting the notebook, set the execution mode as an environment variable:

**PowerShell:**
```powershell
$env:EXECUTION_MODE = "local"  # or "hybrid" or "docker"
```

**Bash (Linux/Mac):**
```bash
export EXECUTION_MODE=local  # or hybrid or docker
```

### 2. Start Required Services

**For Local Mode:**
No external services needed - notebook will start everything automatically.

**For Hybrid Mode:**
```powershell
docker compose up -d nessie seaweedfs
```

**For Docker Mode:**
```powershell
docker compose up -d
```

### 3. Install Jupyter (if not already installed)

```powershell
pip install jupyter
```

## Running the Notebook

### Start Jupyter

```powershell
cd D:\riskanalytics_df
jupyter notebook
```

This will open Jupyter in your browser at `http://localhost:8888`.

### Open the Notebook

Navigate to `notebooks/validation_and_testing.ipynb` and open it.

### Execute the Notebook

Run the cells in order from top to bottom. Each cell is self-contained and provides clear output.

## Notebook Structure

### Part 1: Environment Validation

**Purpose:** Verify the environment is correctly configured for the selected execution mode.

**Checks:**
- Execution mode is valid (local, hybrid, or docker)
- Configuration loads successfully
- Spark session creates successfully
- Catalog connection works
- Namespaces exist

**Expected Output:**
```
✓ Valid execution mode: local
✓ Configuration loaded successfully
✓ Spark session created successfully
✓ Catalog connection successful
✓ Namespace 'risk_analytics_stage' exists
✓ Namespace 'risk_analytics_ods' exists
```

### Part 2: Table Structure Validation

**Purpose:** Verify all required tables are created with correct structure.

**Checks:**
- Stage namespace tables exist
- ODS namespace tables exist
- Source namespace tables exist (Docker mode only)

**Expected Output:**
```
Checking tables in stage namespace...
✓ customer_stage_sourcea
✓ customer_stage_sourceb
✓ asset_stage_sourcea
...
Checking tables in ODS namespace...
✓ customer
✓ asset
✓ collateral
✓ deals
```

### Part 3: Data Loading Validation

**Purpose:** Verify data is loaded correctly in each layer.

**Checks:**
- Stage layer data counts for all entities and sources
- ODS layer data counts for all entities
- Risk metrics data count
- Sample data inspection

**Expected Output:**
```
Validating Stage Layer Data
✓ risk_analytics_stage.customer_stage_sourcea: 5 records
✓ risk_analytics_stage.customer_stage_sourceb: 3 records
...

Validating ODS Layer Data
✓ risk_analytics_ods.customer: 8 records
✓ risk_analytics_ods.asset: 10 records
...
```

### Part 4: Data Quality Checks

**Purpose:** Verify data quality and business logic compliance.

**Checks:**
- Null values in key columns
- Data consistency across layers (stage vs ODS)
- Business logic validation in risk metrics:
  - PFE >= 0
  - VaR >= 0
  - Netting exposure <= gross exposure
  - Risk run ID not null

**Expected Output:**
```
Checking for null values in key columns
✓ risk_analytics_ods.customer.customer_id: No null values
✓ risk_analytics_ods.customer.customer_name: No null values
...

Validating business logic in risk metrics
✓ All PFE values are non-negative
✓ All VaR values are non-negative
✓ Netting exposure <= gross exposure for all records
✓ All records have risk_run_id
```

### Part 5: Performance Validation

**Purpose:** Verify query performance is acceptable.

**Checks:**
- Query execution time for common operations
- Statistics generation

**Expected Output:**
```
Testing query performance
✓ Count all customers: 123.45ms
✓ Count all deals: 234.56ms
✓ Customer distribution by country: 345.67ms
✓ Deal distribution by product: 456.78ms
```

### Part 6: End-to-End Testing

**Purpose:** Run a complete pipeline and validate final outputs.

**Steps:**
1. Bootstrap (create tables)
2. Stage transformations (SourceA)
3. ODS transformations (SourceA)
4. Risk metrics calculation
5. Final output validation

**Expected Output:**
```
End-to-End Pipeline Test
Step 1: Running bootstrap...
✓ Bootstrap completed successfully

Step 2: Running stage transformations...
✓ Stage customer completed successfully
✓ Stage asset completed successfully
...

Step 5: Validating final outputs...
✓ Risk metrics generated: 5 records

VALIDATION COMPLETE
```

## Troubleshooting

### Execution Mode Not Set

**Error:** `Invalid execution mode 'None'`

**Solution:** Set the environment variable before starting Jupyter:
```powershell
$env:EXECUTION_MODE = "local"
```

### Spark Connection Failed

**Error:** `Could not connect to Spark`

**Solution:**
- For local mode: Ensure Java is installed and in PATH
- For hybrid mode: Ensure Nessie and SeaweedFS are running
- For docker mode: Ensure Docker stack is running

### Tables Not Found

**Error:** `Table or view not found`

**Solution:** Run the bootstrap step first:
```powershell
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

### No Data in Tables

**Error:** `Table has 0 records`

**Solution:** Run the data loading pipeline:
```powershell
python jobs/run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18
```

### Risk Metrics Not Generated

**Error:** `Risk metrics table has 0 records`

**Solution:** Run the risk metrics pipeline:
```powershell
python jobs/run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

## Mode-Specific Testing

### Local Mode Testing

**Full Test Sequence:**
```powershell
# Set mode
$env:EXECUTION_MODE = "local"

# Start Jupyter
jupyter notebook

# In notebook, run all cells
# The notebook will handle everything automatically
```

**What's Tested:**
- Local Spark session creation
- Local catalog (memory-based)
- Local storage (file-based)
- Direct file-to-stage data loading
- Simplified architecture (no source tables)

### Hybrid Mode Testing

**Full Test Sequence:**
```powershell
# Start remote services
docker compose up -d nessie seaweedfs

# Set mode
$env:EXECUTION_MODE = "hybrid"

# Start Jupyter
jupyter notebook

# In notebook, run all cells
```

**What's Tested:**
- Local Spark with remote catalog connection
- Nessie catalog connectivity
- SeaweedFS storage connectivity
- Direct file-to-stage data loading
- Remote data persistence

### Docker Mode Testing

**Full Test Sequence:**
```powershell
# Start full stack
docker compose up -d

# Set mode
$env:EXECUTION_MODE = "docker"

# Start Jupyter
jupyter notebook

# In notebook, run all cells
```

**What's Tested:**
- Docker Spark connection
- Full architecture with source tables
- Kafka integration (if enabled)
- Airflow DAG execution
- Complete production-like environment

## Expected Results by Mode

### Local Mode
- **Tables:** Stage + ODS only (no source tables)
- **Data:** Loaded from files directly to stage
- **Storage:** Local file system
- **Catalog:** Memory-based (no persistence)

### Hybrid Mode
- **Tables:** Stage + ODS only (no source tables)
- **Data:** Loaded from files directly to stage
- **Storage:** SeaweedFS (S3-compatible)
- **Catalog:** Nessie (Git-like versioning)

### Docker Mode
- **Tables:** Source + Stage + ODS
- **Data:** Loaded to source tables, then stage, then ODS
- **Storage:** SeaweedFS
- **Catalog:** Nessie
- **Orchestration:** Airflow DAGs

## Automated Testing

For CI/CD or automated testing, you can run the notebook in headless mode:

```powershell
pip install nbconvert
jupyter nbconvert --to script notebooks/validation_and_testing.ipynb
python notebooks/validation_and_testing.py
```

## Validation Checklist

Use this checklist to verify the platform is working correctly:

- [ ] Execution mode is set correctly
- [ ] Required services are running (hybrid/docker modes)
- [ ] Spark session creates successfully
- [ ] Catalog connection works
- [ ] All namespaces exist
- [ ] All tables exist
- [ ] Stage layer has data
- [ ] ODS layer has data
- [ ] Risk metrics are calculated
- [ ] No null values in key columns
- [ ] Data is consistent across layers
- [ ] Business logic validations pass
- [ ] Query performance is acceptable
- [ ] End-to-end pipeline runs successfully

## Next Steps

After successful validation:

1. **Run the actual pipeline** using the runbooks
2. **Monitor execution** using the logging system
3. **Explore data** using the UI or SQL queries
4. **Test additional scenarios** with different dates and sources

For detailed operational procedures, see the [Runbooks](runbooks.md).

For monitoring and logging, see [Logging and Monitoring](logging_and_monitoring.md).