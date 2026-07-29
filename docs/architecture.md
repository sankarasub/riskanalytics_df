# Framework Overview, System Design & Tech Stack

> Quick links: [Overview](index.md) | [Data Model](data-model-risk-metrics.md) | [Interfaces](platform-interfaces-and-operations.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md)

This page combines framework overview and architecture in one place.

## What This Platform Runs

The platform supports two coordinated execution paths:

1. Batch orchestration through Airflow for repeatable end-to-end runs.
2. Event-driven ingestion through Kafka plus Spark Structured Streaming.

Both paths publish to Iceberg tables through Nessie Catalog.

## Architecture Principles

- Separate orchestration, compute, catalog, storage, and interface concerns.
- Keep risk assumptions explicit and configuration-driven.
- Keep pipeline logic metadata-driven through YAML definitions.
- Publish only after successful execution, with run traceability.

## Technology Stack

| Technology | Role |
| --- | --- |
| Apache Spark | Distributed transforms, aggregation, and table writes |
| Apache Iceberg | Analytical table format with partition/snapshot semantics |
| Project Nessie | Git-like versioned catalog for branch-isolated writes |
| SeaweedFS S3 API | Local object warehouse for Iceberg data files |
| Apache Airflow | Batch orchestration and DAG observability |
| Apache Kafka | Event-driven trigger and publish channels |
| FastAPI | Links portal and operational endpoints |
| Streamlit | Business and developer dashboards |
| JupyterLab | Interactive notebook analytics (PySpark) |
| Dremio | SQL exploration over Nessie/Iceberg |
| Docker Compose | Repeatable local multi-service platform |

## End-to-End Architecture

```mermaid
flowchart LR
    user[Business users, analysts, engineers]

    subgraph interfaces[Interfaces]
        links[Links Portal :8000]
        business[Business Dashboard :8501]
        developer[Developer Dashboard :8502]
        airflow[Airflow :8088]
        jupyter[JupyterLab :8888]
        dremio[Dremio :9047]
        kafka_ui[Kafka UI :8090]
    end

    subgraph processing[Processing]
        dags[Airflow DAGs]
        stream[Kafka entity streams]
        spark[Spark master, worker, connect]
        yaml[YAML executor]
    end

    subgraph platform[Platform Services]
        kafka[Kafka broker :9092]
        nessie[Nessie Catalog :19120]
        seaweed[SeaweedFS S3 API :8333]
        postgres[Postgres for Airflow metadata]
    end

    user --> interfaces
    links --> business
    links --> developer
    links --> airflow
    links --> jupyter
    links --> dremio
    links --> kafka_ui

    developer --> dags
    airflow --> dags
    dags --> yaml
    yaml --> spark
    stream --> spark
    kafka --> stream
    kafka --> dags

    spark <--> nessie
    spark <--> seaweed
    airflow --- postgres
    dremio --> nessie
    dremio --> seaweed
    jupyter --> spark
    business --> spark
```

## Runtime Flow

```mermaid
flowchart TD
    start([Start platform]) --> init[Docker Compose starts core services]
    init --> bootstrap[Bootstrap creates namespaces and tables]
    bootstrap --> seed[Seed sample source data]
    seed --> source_ods[Run source-to-ODS stage and ODS loads]
    source_ods --> risk[Run risk pipeline]
    risk --> branch{Nessie branch path available?}
    branch -->|Yes| write_branch[Write metrics on branch]
    branch -->|No| write_main[Write directly to main]
    write_branch --> merge[Merge branch to main]
    write_main --> publish[Publish data and events]
    merge --> publish
    publish --> consume[Dashboards, SQL, notebooks, API]
```

## Execution Framework

Primary runtime entry points:

- Scripted end-to-end run: `scripts/run_risk_analytics_pipeline.ps1`
- Local Python, no Airflow: `scripts/run_local_python_no_airflow.py`
- DAG-triggered batch execution: `ra_createtables_and_data` -> `ra_stage_to_ods_orchestration` -> (`ra_<source>_<entity>_stage` -> `ra_<source>_<entity>_ods`) -> `ra_riskmetrics_eval_ods`
- DAG-triggered streaming execution: `ra_kafka_customer_stage` -> `ra_kafka_customer_ods` -> `ra_riskmetrics_eval_ods`

Core execution modules:

- `risk_analytics/yaml_executor.py`
- `risk_analytics/transformations/components.py`
- `jobs/run_source_to_ods_step.py`
- `jobs/run_risk_pipeline.py`

## Source-to-ODS Model

Namespaces:

- Stage: `nessie.risk_analytics_stage`
- ODS: `nessie.risk_analytics_ods`

Standardized ODS entities:

- `customer`
- `asset`
- `collateral`
- `deals`

Published output:

- `nessie.risk_analytics_ods.risk_metrics`

## Kafka Event Path in This Architecture

Kafka supports near-real-time entity ingestion and trigger signaling for the source-to-ODS architecture.

1. Entity events arrive on ingest topics (customer, asset, collateral, deals).
2. Kafka consumers process micro-batches and land rows in source/stage contracts.
3. Trigger events are published on `risk.pipeline.trigger`.
4. `ra_kafka_customer_stage` (an `AwaitMessageSensor` DAG) consumes the trigger and loads the customer STAGE micro-batch.
5. It triggers `ra_kafka_customer_ods`, which standardizes the micro-batch into the ODS contract.
6. `ra_kafka_customer_ods` triggers `ra_riskmetrics_eval_ods`, which reads ODS entities and publishes `risk_metrics`.

This keeps event-driven flow aligned with the same stage -> ODS -> risk model used by batch orchestration.

## Why This Design Works

- Declarative YAML keeps business transformations readable.
- Shared executor keeps behavior consistent across pipelines.
- Versioned catalog provides safer publication semantics.
- Multiple interfaces serve business, engineering, and analytics users on one governed data model.
