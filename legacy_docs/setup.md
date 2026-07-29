# Local Installation & Quickstart

## Prerequisites

Install the following before starting the platform:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose enabled and running.
- Git, if cloning from a remote repository.
- PowerShell on Windows for the provided orchestration helper.
- Python 3.11+ only when running the unit tests or MkDocs site from the host machine.

Confirm Docker is available:

```powershell
docker --version
docker compose version
```

## Get the Project

```powershell
git clone <repository-url>
cd data_factory
```

If the project is already present locally, open PowerShell in its root directory instead.

## Configure the Environment

Create the local environment file from the supplied template:

```powershell
Copy-Item .env.example .env
```

The template defines the local S3-compatible access keys and Airflow administrator credentials.

| Variable | Purpose |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | SeaweedFS S3 access key used by Spark and service containers. |
| `AWS_SECRET_ACCESS_KEY` | SeaweedFS S3 secret used by Spark and service containers. |
| `AIRFLOW_UID` | Container user identifier for Airflow file permissions. |
| `AIRFLOW_ADMIN_USER` | Local Airflow administrator user name. |
| `AIRFLOW_ADMIN_PASSWORD` | Local Airflow administrator password. |

For a personal development machine, the values in `.env.example` are ready to use. Treat `.env` as local configuration and do not commit environment-specific credentials.

## Start the Platform

Build the images and start all services in the background:

```powershell
docker compose up --build -d
```

Verify service status:

```powershell
docker compose ps --all
```

`storage-init` and `airflow-init` should exit with code `0`; they are intentional one-time initialization services. The long-running services should report `Up`.

## Run the Complete Data Flow

The supplied PowerShell helper starts the platform, initializes tables and sample data, executes source transformations, calculates the risk metrics, and performs a validation query.

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
```

For later executions with a fully cached platform, use strict offline mode. It performs no image pull, build, or dependency download:

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode offline
```

If PowerShell prevents local-script execution, enable it only for the current terminal session:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Manual Execution Option

Create tables and load sample data:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/risk_analytics/jobs/bootstrap.py
```

Run the final metrics calculation:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/risk_analytics/jobs/run_risk_pipeline.py --as-of-date 2026-07-18
```

## Open the Product Interfaces

| Interface | Address | Access |
| --- | --- | --- |
| Business dashboard | [http://localhost:8501](http://localhost:8501) | No sign-in configured for local use. |
| Developer dashboard | [http://localhost:8502](http://localhost:8502) | No sign-in configured for local use. |
| Links and pipeline API | [http://localhost:8000](http://localhost:8000) | No sign-in configured for local use. |
| Airflow | [http://localhost:8088](http://localhost:8088) | Credentials from `.env` (`admin` / `admin` by default). |
| Spark master | [http://localhost:8080](http://localhost:8080) | Monitoring UI. |
| JupyterLab | [http://localhost:8888](http://localhost:8888) | Local development access. |
| Dremio | [http://localhost:9047](http://localhost:9047) | Create a local account on first visit. |
| Nessie API | [http://localhost:19120](http://localhost:19120) | Catalog API. |

## Run Verification

Install host-side development dependencies in an activated virtual environment, then run the test suite:

```powershell
python -m pip install -r requirements\ui.txt
python -m unittest discover -s tests -v
```

Run a platform health check from a running container:

```powershell
docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py --check-iceberg
```

Serve the MkDocs site locally:

```powershell
python -m pip install -r requirements\docs.txt
mkdocs serve
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000) to view the documentation.

## Stop the Platform

Stop the containers while preserving named-volume data:

```powershell
docker compose down
```
