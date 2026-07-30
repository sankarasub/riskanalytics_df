# Framework Overview, System Design & Tech Stack

> Quick links: [Overview](index.md) | [Data Model](data-model-risk-metrics.md) | [Interfaces](platform-interfaces-and-operations.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md)

This page is the single architecture reference: framework overview, system design, technology stack,
and every architecture diagram. It absorbed the former `ARCHITECTURE_DIAGRAM.md` and
`docs/architecture-diagram.md`, which duplicated this content. Table-level contracts and metric
formulas live in [Data Model and Risk Metrics](data-model-risk-metrics.md); the per-file inventory
lives in the [Repository File Guide](project-reference.md).

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
        links[Operations API and links portal :8000]
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

Service ports: business UI `8501`, developer UI `8502`, operations API `8000`, Airflow `8088`,
JupyterLab `8888`, Dremio `9047`, Kafka UI `8090`, Nessie `19120`, SeaweedFS S3 `8333`, Spark master
`7077` (UI `8080`), Spark worker UI `8081`, Spark Connect `15002`, and Kafka `29092` from the host
(`9092` inside the Compose network).

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
- DAG-triggered streaming execution, per entity: `ra_kafka_<entity>_stage` -> `ra_kafka_<entity>_ods` -> `ra_riskmetrics_eval_ods` for `customer`, `asset`, `collateral`, and `deals`

Core execution modules:

- `risk_analytics/yaml_executor.py`
- `risk_analytics/transformations/components.py`
- `jobs/run_source_to_ods_step.py`
- `jobs/run_risk_pipeline.py`

## Batch Orchestration Flow

```mermaid
flowchart TD
    Start["Start Orchestration<br/>ra_stage_to_ods_orchestration"]

    subgraph GroupA["TaskGroup sourceA_customer (1 of 8, all run concurrently)"]
        StageA["Trigger + deferred wait<br/>ra_sourceA_customer_stage"] --> ODSA["Trigger + deferred wait<br/>ra_sourceA_customer_ods"]
    end

    subgraph GroupN["TaskGroup &lt;source&gt;_&lt;entity&gt;"]
        StageN["Trigger + deferred wait<br/>ra_&lt;source&gt;_&lt;entity&gt;_stage"] --> ODSN["Trigger + deferred wait<br/>ra_&lt;source&gt;_&lt;entity&gt;_ods"]
    end

    Start --> StageA
    Start --> StageN
    ODSA --> AllComplete["All stage/ODS loads complete"]
    ODSN --> AllComplete

    AllComplete --> RiskTrigger["Trigger DAG<br/>ra_riskmetrics_eval_ods"]
    RiskTrigger --> RiskSpark["Spark job:<br/>run_risk_pipeline.py"]

    StageN --> StageSpark["Spark job in the leaf DAG:<br/>run_source_to_ods_step.py --layer stage<br/>pool: spark_submit"]
    ODSN --> ODSSpark["Spark job in the leaf DAG:<br/>run_source_to_ods_step.py --layer ods<br/>pool: spark_submit"]
```

The eight task groups are independent, so STAGE and ODS work for different source/entity pairs
overlaps. Concurrency is bounded by the `spark_submit` Airflow pool (`SPARK_SUBMIT_POOL_SLOTS`,
default 2) rather than by the DAG structure, because every leaf task starts a local Spark driver in
the Airflow container. The waits are deferred, so the `airflow-triggerer` service must run.

DAG identifiers generated by the STAGE and ODS factories (`airflow/dags/ra_stage_jobs.py`,
`airflow/dags/ra_ods_jobs.py`):

| Source | STAGE DAGs | ODS DAGs |
| --- | --- | --- |
| Source A | `ra_sourceA_customer_stage`, `ra_sourceA_asset_stage`, `ra_sourceA_collateral_stage`, `ra_sourceA_deals_stage` | `ra_sourceA_customer_ods`, `ra_sourceA_asset_ods`, `ra_sourceA_collateral_ods`, `ra_sourceA_deals_ods` |
| Source B | `ra_sourceB_customer_stage`, `ra_sourceB_asset_stage`, `ra_sourceB_collateral_stage`, `ra_sourceB_deals_stage` | `ra_sourceB_customer_ods`, `ra_sourceB_asset_ods`, `ra_sourceB_collateral_ods`, `ra_sourceB_deals_ods` |

## YAML Pipeline Execution

```mermaid
sequenceDiagram
    participant User as User/Orchestrator
    participant Job as Spark Job
    participant Executor as YAML Executor
    participant Config as Config
    participant Spark as Spark Session
    participant Nessie as Nessie Catalog
    participant Storage as SeaweedFS

    User->>Job: Trigger with parameters
    Job->>Config: Load configuration
    Job->>Spark: Create Spark session
    Job->>Executor: run_pipeline_from_yaml()

    Executor->>Executor: Load YAML file
    Executor->>Executor: Render templates with params
    Executor->>Executor: Validate pipeline structure

    loop For each source
        Executor->>Spark: Load source (table/file/SQL)
        Spark->>Nessie: Query catalog
        Nessie-->>Spark: Return metadata
        Spark->>Storage: Read data
        Storage-->>Spark: Return DataFrame
    end

    loop For each step
        Executor->>Executor: execute_component()
        Executor->>Spark: Apply transformation
        Spark-->>Executor: Return transformed DataFrame
    end

    loop For each target
        Executor->>Spark: Write to Iceberg table
        Spark->>Nessie: Update catalog
        Spark->>Storage: Write data files
    end

    Executor-->>Job: PipelineExecutionResult
    Job-->>User: Completion status
```

## Branch-Isolated Risk Publication

```mermaid
sequenceDiagram
    participant Orchestrator as Airflow/Script
    participant RiskJob as run_risk_pipeline.py
    participant Nessie as Nessie Client
    participant Spark as Spark Session
    participant Executor as YAML Executor
    participant Kafka as Kafka (optional)

    Orchestrator->>RiskJob: Execute with as_of_date
    RiskJob->>RiskJob: Generate unique run_id
    RiskJob->>Nessie: Create branch "risk-run-{run_id}"
    Nessie-->>RiskJob: Branch created

    RiskJob->>Spark: Create session with ref=branch
    RiskJob->>Executor: run_pipeline_from_yaml()
    Executor->>Spark: Execute transformations on branch
    Spark->>Nessie: Write to branch reference

    Executor-->>RiskJob: Return row counts
    RiskJob->>Spark: Stop session

    RiskJob->>Nessie: Merge branch into main
    Nessie-->>RiskJob: Merge successful

    RiskJob->>Kafka: Publish metrics event (optional)
    RiskJob-->>Orchestrator: Pipeline complete
```

When the Nessie client is unavailable the job writes `main` directly, so a local run still produces
metrics.

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
2. `jobs/kafka_entity_consumer.py` processes micro-batches and lands rows in source/stage contracts.
3. One trigger event per entity touched by the micro-batch is published on `risk.pipeline.trigger`, carrying `entity`, `as_of_date`, and `source` (see `risk_analytics/kafka_events.py`).
4. `ra_kafka_<entity>_stage` (an `AwaitMessageSensor` DAG per entity) matches only its own entity's events and loads that STAGE micro-batch.
5. It triggers `ra_kafka_<entity>_ods`, which standardizes the micro-batch into the ODS contract.
6. The stage DAG then triggers `ra_riskmetrics_eval_ods`, which reads ODS entities and publishes `risk_metrics`. When a Kafka ODS DAG is triggered on its own, it triggers the metrics evaluation itself.

The sensors defer while waiting, so the `airflow-triggerer` service has to be running.

```mermaid
flowchart LR
    Producer["Producer<br/>(Developer UI / API / external)"] --> Topic["Kafka topics<br/>risk.&lt;entity&gt;.ingest"]
    Topic --> Consumer["kafka-entity-stream<br/>jobs/kafka_entity_consumer.py"]
    Consumer --> Landed["Iceberg source tables<br/>nessie.risk_analytics.&lt;entity&gt;"]
    Consumer --> Trigger["Kafka topic risk.pipeline.trigger<br/>one event per entity in the micro-batch"]
    Trigger --> StageDag["DAG ra_kafka_&lt;entity&gt;_stage<br/>AwaitMessageSensor filtered on entity"]
    StageDag --> StageJob["Spark: run_source_to_ods_step.py<br/>--layer stage --entity &lt;entity&gt;"]
    StageJob --> OdsDag["DAG ra_kafka_&lt;entity&gt;_ods"]
    OdsDag --> OdsJob["Spark: run_source_to_ods_step.py<br/>--layer ods --entity &lt;entity&gt;"]
    OdsJob --> Metrics["DAG ra_riskmetrics_eval_ods"]
    Metrics --> Published["Kafka topic<br/>risk.metrics.published"]
```

Streaming reuses the batch tables instead of adding streaming-only ones - see
[Kafka Streaming Contracts](data-model-risk-metrics.md#kafka-streaming-contracts) for the
topic-to-table mapping and the event payloads. This keeps the event-driven flow aligned with the same
stage -> ODS -> risk model used by batch orchestration.

## Key Design Patterns

| Pattern | Purpose | How it is implemented |
| --- | --- | --- |
| YAML-driven transformations | Change pipeline logic without code changes | `risk_analytics/yaml_executor.py` renders and validates the YAML, then dispatches steps (`join`, `lookup`, `rollup`, `reformat`, `filter`, `normalize`, `dedup`) |
| Branch isolation | Keep an incomplete run out of published data | Create a Nessie branch, write, merge only on success; fall back to `main` when Nessie is unavailable |
| Layered data architecture | Separate source shape from business contract | Stage (source-specific) -> ODS (standardized) -> `risk_metrics` (published) |
| Component-based framework | Add transformations without touching the executor | `execute_component()` routes to a handler; each returns `(output, used, unused)` DataFrames |
| Configuration-driven risk | Keep assumptions auditable | Multiplier, z-score, volatility, and haircuts come from `config/platform.yaml` |

## Safety Model

- Every risk run writes on its own Nessie branch and merges only after success.
- Bootstrap DDL and seeding are idempotent, so re-running is safe.
- Optional dependencies (Nessie client, Kafka publication) degrade instead of failing a run.
- `scripts/health_check.py` and `scripts/validate_pipeline_status.py` verify service and data
  readiness before and after a run - see [Scripts Reference](scripts-reference.md).

## Why This Design Works

- Declarative YAML keeps business transformations readable.
- Shared executor keeps behavior consistent across pipelines.
- Versioned catalog provides safer publication semantics.
- Multiple interfaces serve business, engineering, and analytics users on one governed data model.
