# Run Without Airflow (Docker + Local Python)

Yes, you can run this project without Airflow.

This workflow keeps infrastructure in Docker (Spark, Nessie, SeaweedFS, etc.) and executes jobs directly from your local Python environment.

Primary entrypoint:

- `scripts/run_local_python_no_airflow.py` (single command end-to-end run)

## Scope

This runbook covers:

- Start Docker stack (fresh/new build or reuse existing images)
- Run Python jobs locally to create tables and load seed data
- Run canonical transforms manually
- Query top 5 rows from canonical tables
- Run final risk transform and query top 5 output rows

## Quick run (single Python command)

1. Open PowerShell at repo root.
2. Prepare local Python once.
3. Run the orchestrator script.

```powershell
cd C:\Users\Sankar\OneDrive\Documents\data_factory
python .\setup_venv.py
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --docker-mode fresh
```

For subsequent runs (reuse existing images):

```powershell
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --docker-mode reuse
```

Optional flags:

- `--source-mode sourcea|sourceb|both` to choose transform set.
- `--include-sourceb` is supported as a backward-compatible alias for `--source-mode both`.
- `--docker-mode none` if the Docker stack is already up.
- `--spark-remote` and `--nessie-uri` to override endpoints.
- `--run-info-dir` to change where markdown run reports are written.
- `--skip-run-info` to skip writing a markdown run report.

The script runs all requested steps:

- Docker startup (`fresh`, `reuse`, or `none`)
- Bootstrap create/load
- Canonical transforms (SourceA by default)
- Canonical top-5 checks
- Final risk transform
- Final output top-5 checks
- Markdown run report with run metadata and row-count checks

## Run SourceA and SourceB separately

SourceA only (default):

```powershell
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --docker-mode reuse --source-mode sourcea
```

SourceB only:

```powershell
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --docker-mode reuse --source-mode sourceb
```

Both SourceA and SourceB:

```powershell
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --docker-mode reuse --source-mode both
```

## Run Info Markdown Output

By default, each run writes a markdown report under `logs/run-info`.

Example output path:

- `logs/run-info/local_no_airflow_run_20260728_114500.md`

The report includes:

- run timestamp, start/end time, duration
- input parameters (`as_of_date`, `docker_mode`, endpoints)
- generated `run_id`
- canonical table row counts
- `risk_metrics` row count for the requested date

To set a custom report directory:

```powershell
.\.venv\Scripts\python.exe .\scripts\run_local_python_no_airflow.py --as-of-date 2026-07-18 --run-info-dir logs/custom-run-reports
```

## Manual step-by-step (reference)

Use this if you want to execute each command yourself.

### 1. Open PowerShell at repo root

```powershell
cd C:\Users\Sankar\OneDrive\Documents\data_factory
```

### 2. Start Docker services

Choose one mode.

### A) Fresh/new build

Use this if you changed Dockerfiles/dependencies or want a clean platform state.

```powershell
docker compose down -v
docker compose up --build -d
docker compose ps --all
```

### B) Reuse existing build

Use this for faster regular runs.

```powershell
docker compose up -d --no-build
docker compose ps --all
```

### 3. Prepare local Python

Use Python 3.11 to match container runtime.

```powershell
python .\setup_venv.py
$Py = ".\\.venv\\Scripts\\python.exe"
```

If your virtual environment already exists, set only `$Py`.

### 4. Configure local runtime env vars

These make local Python use Spark Connect and host-exposed services.

```powershell
$env:SPARK_REMOTE = "sc://localhost:15002"
$env:NESSIE_URI = "http://localhost:19120/api/v2"
```

### 5. Create tables and load seed data

```powershell
& $Py .\jobs\bootstrap.py --action all --as-of-date 2026-07-18
```

### 6. Run source-to-ODS stage and merge steps (SourceA)

This creates/updates standardized ODS tables from seeded source tables.

```powershell
$entities = @("customer", "asset", "collateral", "deals")
foreach ($entity in $entities) {
  & $Py .\jobs\run_source_to_ods_step.py --layer stage --entity $entity --source sourcea --as-of-date 2026-07-18
  if ($LASTEXITCODE -ne 0) { throw "Stage load failed: $entity sourcea" }
  & $Py .\jobs\run_source_to_ods_step.py --layer ods --entity $entity --source sourcea --as-of-date 2026-07-18
  if ($LASTEXITCODE -ne 0) { throw "ODS load failed: $entity sourcea" }
}
```

### 7. Check ODS data (top 5)

```powershell
@'
from risk_analytics.spark import create_spark_session

spark = create_spark_session("local-canonical-check")
tables = [
  "customer",
  "asset",
  "collateral",
  "deals",
]

for table in tables:
    print(f"\n=== {table} (top 5) ===")
  spark.sql(f"SELECT * FROM nessie.risk_analytics_ods.{table} LIMIT 5").show(truncate=False)

spark.stop()
'@ | & $Py
```

### 8. Run final risk transform

```powershell
& $Py .\jobs\run_risk_pipeline.py --as-of-date 2026-07-18 --run-id local-manual-20260718 --data-model source-to-ods
```

### 9. Check final output (top 5)

```powershell
@'
from risk_analytics.spark import create_spark_session

spark = create_spark_session("local-risk-output-check")

print("\n=== risk_metrics count for 2026-07-18 ===")
spark.sql("""
SELECT COUNT(*) AS row_count
FROM nessie.risk_analytics_ods.risk_metrics
WHERE as_of_date = DATE '2026-07-18'
""").show(truncate=False)

print("\n=== risk_metrics top 5 for 2026-07-18 ===")
spark.sql("""
SELECT *
FROM nessie.risk_analytics_ods.risk_metrics
WHERE as_of_date = DATE '2026-07-18'
ORDER BY calculation_timestamp DESC
LIMIT 5
""").show(truncate=False)

spark.stop()
'@ | & $Py
```

## Optional: Run SourceB stage/ODS steps too

SourceB steps need explicit file-path parameters, for example:

```powershell
& $Py .\jobs\run_source_to_ods_step.py --layer stage --entity customer --source sourceb --as-of-date 2026-07-18 --param customer_sourceb_path="/opt/risk_analytics/data/sourceb/customer/*.csv"
& $Py .\jobs\run_source_to_ods_step.py --layer ods --entity customer --source sourceb --as-of-date 2026-07-18 --param customer_sourceb_path="/opt/risk_analytics/data/sourceb/customer/*.csv"
```

Repeat for `asset_sourceb_path`, `product_sourceb_path`, `trans_sourceb_path`, and `collateral_sourceb_path` when executing corresponding SourceB steps.

## Troubleshooting notes

- If you see Spark Connect package errors locally, rerun `python .\setup_venv.py` to align host dependencies.
- If a run fails with stale state and you want a clean retry, use the fresh/new build sequence (`docker compose down -v`, then `docker compose up --build -d`).
- If you switch back to container `spark-submit` jobs, unset `SPARK_REMOTE` in that shell:

```powershell
Remove-Item Env:SPARK_REMOTE
```