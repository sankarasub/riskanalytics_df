# Dependency Download, Cache, and Images Guide

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md) | [Troubleshooting](troubleshooting.md)

This guide explains images, dependency download paths, cache persistence, and operational checks.

## Images Used by the Platform

| Service group | Image | Build source |
| --- | --- | --- |
| Airflow | `risk-analytics/airflow:2.10.5` | `docker/airflow/Dockerfile` |
| Spark (master/worker/connect/kafka-stream) | `risk-analytics/spark:4.1.3` | `docker/spark/Dockerfile` |
| Notebook | `risk-analytics/notebook:3.11.11` | `docker/notebook/Dockerfile` |
| UI and API | `risk-analytics/ui:3.11.11` | `docker/ui/Dockerfile` |
| Nessie | `ghcr.io/projectnessie/nessie:0.104.3` | upstream |
| SeaweedFS | `chrislusf/seaweedfs:3.99` | upstream |
| Dremio | `dremio/dremio-oss:26.0.5` | upstream |
| Kafka | `confluentinc/cp-kafka:7.7.1` | upstream |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | upstream |
| Postgres | `postgres:16-alpine` | upstream |

## Runtime Dependency Downloads

### Spark package downloads

Spark jobs that use `--packages` download Maven artifacts (through Ivy).

Cache path in containers:

- `/opt/spark/.ivy2`

Persistent volume:

- `spark-ivy-cache` mounted to `/opt/spark/.ivy2`

Services using this cache:

- `spark-master`
- `spark-worker`
- `spark-connect`
- `kafka entity stream consumer service`

### Python dependencies inside images

Python packages are primarily installed at image build time from Dockerfiles and requirements manifests.

- Airflow image: dependency install in `docker/airflow/Dockerfile`
- Spark image: dependency install in `docker/spark/Dockerfile`
- UI/API image: dependency install in `docker/ui/Dockerfile`
- Notebook image: dependency install in `docker/notebook/Dockerfile`

## Build-Time vs Runtime Behavior

| Behavior | Build-time image layer | Runtime volume cache |
| --- | --- | --- |
| Python package install | Yes | No |
| Bundled Spark jars from image build | Yes | No |
| Spark `--packages` downloads | No | Yes (`spark-ivy-cache`) |
| Kafka topic metadata/state | No | Yes (`kafka-data`) |
| Airflow metadata DB | No | Yes (`postgres-data`) |

## Named Volumes and Purpose

| Volume | Purpose |
| --- | --- |
| `spark-ivy-cache` | Spark Ivy/Maven package cache |
| `kafka-data` | Kafka broker log/state persistence |
| `postgres-data` | Airflow metadata persistence |
| `dremio-data` | Dremio metadata/state |
| `seaweed-data` | SeaweedFS object-storage data |

## Configuration Keys That Affect Dependency Fetching

- `KAFKA_BOOTSTRAP_SERVERS`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `SPARK_REMOTE`
- `SPARK_MASTER_URL`

Network and package coordinates in Spark commands also affect whether new downloads occur.

## Commands to Inspect Cache and Downloads

```powershell
docker volume ls
docker volume inspect risk-analytics-lakehouse_spark-ivy-cache
docker compose exec spark-master sh -lc "ls -lah /opt/spark/.ivy2; ls -lah /opt/spark/.ivy2/cache || true"
docker compose exec spark-master sh -lc "ls -lah /opt/spark/jars | head -n 80"
docker compose exec airflow-webserver pip show apache-airflow pyspark confluent-kafka
```

## First-Build and Offline Strategy

First build and cache warm-up:

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
```

Offline-style regular runs:

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode offline
```

## Why Re-Downloads Happen

Expected scenarios:

- First run on a new machine
- Changed Spark package coordinates or versions
- Cache volume removed (`docker compose down -v`)
- New image build with changed dependencies
- Docker Desktop data cleanup

## Recovery and Optimization Tips

- Prefer `offline` mode for regular runs.
- Rebuild only impacted services instead of full stack.
- Keep `spark-ivy-cache` volume intact unless debugging dependency corruption.
- If cache appears corrupted, remove only the affected volume and rerun first-build once.
