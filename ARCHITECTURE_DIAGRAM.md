# Risk Analytics Lakehouse - Architecture Diagram

## System Overview

This is a production-oriented data platform for financial risk analytics that implements a modern lakehouse architecture using Apache Spark, Apache Iceberg, Project Nessie, and SeaweedFS.

## High-Level Architecture

```mermaid
flowchart TB
    subgraph "User Access Layer"
        BusinessUI["Business Dashboard<br/>Streamlit :8501"]
        DeveloperUI["Developer Control Plane<br/>Streamlit :8502"]
        LinksAPI["Links Portal<br/>FastAPI :8000"]
        JupyterLab["JupyterLab Notebook<br/>:8888"]
        DremioUI["Dremio SQL UI<br/>:9047"]
        AirflowUI["Airflow Web UI<br/>:8088"]
        KafkaUI["Kafka UI<br/>:8090"]
    end

    subgraph "Orchestration Layer"
        AirflowWeb["Airflow Webserver"]
        AirflowScheduler["Airflow Scheduler"]
        Postgres["PostgreSQL<br/>Metadata Store"]
    end

    subgraph "Compute Layer"
        SparkMaster["Spark Master<br/>:7077/:8080"]
        SparkWorker["Spark Worker<br/>:8081"]
        SparkConnect["Spark Connect Server<br/>:15002"]
    end

    subgraph "Streaming Layer"
        Kafka["Kafka Broker<br/>:29092"]
        KafkaConsumer["Kafka Entity Consumer<br/>Spark Structured Streaming"]
    end

    subgraph "Lakehouse Layer"
        Nessie["Project Nessie<br/>Iceberg Catalog<br/>:19120"]
        SeaweedFS["SeaweedFS<br/>S3 Storage<br/>:8333"]
    end

    subgraph "Data Pipeline Jobs"
        Bootstrap["Bootstrap Job<br/>Table Creation + Seed Data"]
        StageODS["Source-to-ODS Jobs<br/>Stage → ODS Transform"]
        RiskPipeline["Risk Metrics Pipeline<br/>Final Calculations"]
    end

    subgraph "Transformation Framework"
        YAMLExecutor["YAML Pipeline Executor"]
        Components["Transformation Components<br/>join, lookup, rollup, reformat, filter"]
    end

    %% User Access Connections
    BusinessUI --> Nessie
    BusinessUI --> SparkConnect
    DeveloperUI --> Nessie
    DeveloperUI --> SparkConnect
    JupyterLab --> SparkConnect
    DremioUI --> Nessie
    DremioUI --> SeaweedFS
    AirflowUI --> AirflowWeb
    KafkaUI --> Kafka
    LinksAPI --> BusinessUI
    LinksAPI --> DeveloperUI
    LinksAPI --> JupyterLab
    LinksAPI --> DremioUI
    LinksAPI --> AirflowUI
    LinksAPI --> KafkaUI

    %% Orchestration Connections
    AirflowWeb --> Postgres
    AirflowScheduler --> Postgres
    AirflowWeb --> SparkMaster
    AirflowScheduler --> SparkMaster

    %% Compute Connections
    SparkMaster --> SparkWorker
    SparkConnect --> SparkMaster
    SparkConnect --> Nessie
    SparkConnect --> SeaweedFS
    SparkMaster --> Nessie
    SparkMaster --> SeaweedFS

    %% Streaming Connections
    KafkaConsumer --> Kafka
    KafkaConsumer --> SparkMaster
    KafkaConsumer --> Nessie
    KafkaConsumer --> SeaweedFS

    %% Pipeline Connections
    Bootstrap --> SparkMaster
    StageODS --> SparkMaster
    RiskPipeline --> SparkMaster

    %% Transformation Framework
    YAMLExecutor --> Components
    StageODS --> YAMLExecutor
    RiskPipeline --> YAMLExecutor

    %% Data Flow
    Kafka -.->|Ingest| StageODS
    Bootstrap -->|Initialize Tables| Nessie
    StageODS -->|Write| Nessie
    RiskPipeline -->|Write with Branch Isolation| Nessie
```

## Data Pipeline Flow

```mermaid
flowchart LR
    subgraph "Source Systems"
        SourceA["SourceA<br/>JSON Files"]
        SourceB["SourceB<br/>CSV/JSON Files"]
        KafkaTopics["Kafka Topics<br/>risk.*.ingest"]
    end

    subgraph "Stage Layer"
        StageCustomer["stage_customer_sourcea"]
        StageAsset["stage_asset_sourcea"]
        StageCollateral["stage_collateral_sourcea"]
        StageDeals["stage_deals_sourcea"]
    end

    subgraph "ODS Layer"
        ODSCustomer["ods_customer"]
        ODSAsset["ods_asset"]
        ODSCollateral["ods_collateral"]
        ODSDeals["ods_deals"]
    end

    subgraph "Risk Metrics"
        RiskMetrics["risk_metrics<br/>PFE, VaR, Exposure"]
    end

    subgraph "Processing"
        YAMLTransform["YAML Transformations"]
        SparkJobs["Spark Jobs"]
    end

    SourceA --> SparkJobs
    SourceB --> SparkJobs
    KafkaTopics --> SparkJobs

    SparkJobs --> YAMLTransform
    YAMLTransform --> StageCustomer
    YAMLTransform --> StageAsset
    YAMLTransform --> StageCollateral
    YAMLTransform --> StageDeals

    StageCustomer --> SparkJobs
    StageAsset --> SparkJobs
    StageCollateral --> SparkJobs
    StageDeals --> SparkJobs

    SparkJobs --> YAMLTransform
    YAMLTransform --> ODSCustomer
    YAMLTransform --> ODSAsset
    YAMLTransform --> ODSCollateral
    YAMLTransform --> ODSDeals

    ODSCustomer --> SparkJobs
    ODSAsset --> SparkJobs
    ODSCollateral --> SparkJobs
    ODSDeals --> SparkJobs

    SparkJobs --> YAMLTransform
    YAMLTransform --> RiskMetrics
```

## Component Interaction Details

### 1. YAML-Driven Pipeline Execution

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

### 2. Branch-Isolated Risk Pipeline

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
    Spark->>Storage: Write data files

    Executor-->>RiskJob: Return row counts
    RiskJob->>Spark: Stop session

    RiskJob->>Nessie: Merge branch → main
    Nessie-->>RiskJob: Merge successful

    RiskJob->>Kafka: Publish metrics event (optional)
    RiskJob-->>Orchestrator: Pipeline complete
```

### 3. Airflow Orchestration Flow

```mermaid
flowchart TD
    Start["Start Orchestration"] --> Trigger["Trigger DAG<br/>risk_analytics_source_to_ods_orchestration"]

    Trigger --> LoopStart["Loop: entities × sources"]
    LoopStart --> StageTask["Trigger DAG<br/>risk_analytics_stage_load"]
    StageTask --> StageWait["Wait for completion"]
    StageWait --> ODSTask["Trigger DAG<br/>risk_analytics_ods_load"]
    ODSTask --> ODSWait["Wait for completion"]

    ODSWait --> MoreEntities{"More entities?"}
    MoreEntities -->|Yes| LoopStart
    MoreEntities -->|No| AllComplete["All stage/ODS loads complete"]

    AllComplete --> RiskTrigger["Trigger DAG<br/>risk_analytics_pipeline"]
    RiskTrigger --> RiskComplete["Risk pipeline complete"]
    RiskComplete --> End["End"]

    StageTask --> StageSpark["Spark Job:<br/>run_source_to_ods_step.py --layer stage"]
    ODSTask --> ODSSpark["Spark Job:<br/>run_source_to_ods_step.py --layer ods"]
    RiskTrigger --> RiskSpark["Spark Job:<br/>run_risk_pipeline.py"]
```

## Key Design Patterns

### 1. YAML-Driven Transformations
- **Purpose**: Declarative pipeline definitions without code changes
- **Components**: join, lookup, rollup, reformat, filter, normalize, dedup
- **Benefits**: Version-controlled, readable, testable without Spark

### 2. Branch Isolation
- **Purpose**: Prevent incomplete runs from affecting published data
- **Implementation**: Create Nessie branch → Write → Merge on success
- **Fallback**: Direct main write if Nessie unavailable

### 3. Layered Data Architecture
- **Stage**: Raw source data with minimal transformation
- **ODS**: Standardized business entities across sources
- **Metrics**: Final risk calculations (PFE, VaR, exposure)

### 4. Component-Based Framework
- **Dispatcher**: `execute_component()` routes to transformation handlers
- **Contract**: Each component returns (output, used, unused) DataFrames
- **Extensibility**: Add new transformations via component registry

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Storage | SeaweedFS | S3-compatible object storage |
| Catalog | Nessie | Iceberg catalog with Git-like versioning |
| Format | Apache Iceberg | ACID transactions, time travel, schema evolution |
| Compute | Apache Spark 4.1.3 | Distributed data processing |
| Orchestration | Apache Airflow 2.10.5 | Workflow scheduling |
| Streaming | Kafka | Real-time data ingestion |
| Query | Dremio | SQL query engine for Iceberg |
| UI | Streamlit | Business/developer dashboards |
| Notebooks | JupyterLab | Interactive analysis |
| Language | Python 3.11 | Primary implementation language |

## Data Model

### Entities
- **Customer**: Counterparty information with ratings and entity types
- **Asset**: Financial instruments with ISIN, asset class, valuations
- **Collateral**: Collateral agreements with asset assignments
- **Deals**: Trade positions with product details and risk factors

### Risk Metrics
- **Gross Exposure**: Sum of trade exposures before netting
- **Netting Exposure**: Net exposure after netting set aggregation
- **Collateral Value**: Collateral adjusted by haircuts
- **PFE (Potential Future Exposure)**: Stress-tested exposure with multiplier
- **VaR (Value at Risk)**: Volatility-adjusted exposure with confidence interval

## File Structure

```
data_factory/
├── airflow/dags/              # Airflow DAG definitions
├── jobs/                      # Spark job entry points
│   ├── bootstrap.py          # Table creation + seed data
│   ├── run_source_to_ods_step.py  # Stage/ODS transformation
│   ├── run_risk_pipeline.py   # Final risk metrics calculation
│   └── kafka_entity_consumer.py  # Kafka streaming consumer
├── risk_analytics/            # Core framework
│   ├── yaml_executor.py       # YAML pipeline execution engine
│   ├── transformations/       # Transformation components
│   ├── config.py              # Configuration management
│   ├── spark.py               # Spark session factory
│   └── nessie.py              # Nessie client wrapper
├── transform/                 # YAML pipeline definitions
│   ├── source_to_ods/         # Stage/ODS transformation YAMLs
│   └── risk_metrics_pipeline_source_to_ods.yaml
├── data/                      # Sample data files
├── ui/                        # Streamlit applications
├── api/                       # FastAPI links portal
└── docker-compose.yml         # Service orchestration
```

## Execution Modes

1. **Manual Execution**: Direct spark-submit commands
2. **Airflow Orchestration**: DAG-based scheduling
3. **Streaming**: Kafka-triggered pipeline execution
4. **Helper Script**: PowerShell script for complete pipeline runs

## Safety Model

- **Branch Isolation**: Each risk run creates a Nessie branch
- **Merge on Success**: Only successful runs merge to main
- **Idempotent Operations**: Bootstrap safe to repeat
- **Validation**: Health checks for service connectivity
- **Fallback**: Graceful degradation when optional services unavailable
