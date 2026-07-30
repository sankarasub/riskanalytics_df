# Risk Analytics Lakehouse

> Quick links: [Architecture](architecture.md) | [Data Model](data-model-risk-metrics.md) | [Interfaces](platform-interfaces-and-operations.md) | [Runbook](runbooks.md) | [Production Setup](production_setup.md) | [Testing](testing.md)

## Platform Delivery

Risk Analytics Lakehouse is a containerized data platform that turns heterogeneous customer, asset, collateral, and deal data into transparent, queryable counterparty-risk metrics. It provides a complete local delivery environment: ingestion, transformation, versioned lakehouse storage, orchestration, operational checks, and stakeholder-facing access points all run together through Docker Compose.

This platform addresses a common analytics challenge: risk calculations depend on data that arrives in different shapes and formats, while outputs must be reproducible, auditable, and easy to explore. The platform standardizes inputs into governed Iceberg tables, applies declarative transformation definitions, and publishes metrics only after the calculation path completes successfully.

## Delivered Capabilities

- **Multi-source data standardization.** Source A and Source B records are loaded from JSON and CSV inputs into staging and canonical lakehouse tables.
- **Declarative transformation framework.** YAML pipeline definitions express joins, lookups, filters, reshaping, aggregation, normalization, deduplication, and target write behavior without hard-coding each data flow in Python.
- **Risk metric calculation.** The delivered risk pipeline calculates gross exposure, netting exposure, collateral after haircut, potential future exposure (PFE), and value at risk (VaR).
- **Version-aware publishing.** Each risk run can execute on an isolated Nessie branch and merge into the shared `main` reference once the write path succeeds.
- **Operational orchestration.** Airflow DAGs coordinate bootstrap, source transformations, and final metric publication. The eight source/entity chains fan out concurrently, and per-entity Kafka sensor DAGs give every entity an event-driven STAGE -> ODS -> risk-metrics path.
- **Accessible analytics.** Streamlit dashboards, JupyterLab, Dremio, and a FastAPI operations API (health, catalog listing, pipeline execution, plus the links portal) provide business, engineering, notebook, SQL, and service-discovery access.
- **Verification built in.** Unit tests validate configuration, Spark session setup, YAML execution semantics, bootstrap behavior, Nessie interactions, health checks, Kafka event handling, and orchestration command construction. CI additionally runs ruff, mypy, a real Airflow DAG parse, and a strict docs build.

## Platform Outcomes

The delivery demonstrates production-minded data engineering practices in a portfolio-ready form:

- A reproducible local platform rather than an isolated script.
- Separation between domain rules, orchestration, infrastructure, and presentation.
- Configurable assumptions for haircuts, volatility defaults, PFE, and VaR.
- Traceable outputs carrying run identity, as-of date, calculation timestamp, and source branch.
- A clear path from raw source files to governed, explorable analytical data.

## Primary Interfaces

| Audience | Interface | Purpose |
| --- | --- | --- |
| Business user | Streamlit business dashboard | View risk metrics and summary indicators. |
| Data engineer | Streamlit developer dashboard and FastAPI API | Validate, preview, and execute YAML transformations. |
| Platform operator | Airflow and health-check script | Run, monitor, and verify the platform. |
| Analyst | JupyterLab and Dremio | Query published Iceberg tables through notebooks or SQL. |

## Documentation Map

- [Framework Overview, System Design & Tech Stack](architecture.md): architecture, runtime flow, and execution paths.
- [Architecture Diagrams](architecture-diagram.md): component, orchestration, streaming, and lineage diagrams.
- [Data Model and Risk Metrics](data-model-risk-metrics.md): table contracts, lineage, formulas, and worked examples.
- [Platform Interfaces and Operations](platform-interfaces-and-operations.md): dashboard, orchestration, SQL, notebook, and messaging interfaces.
- [Runbook and Local Execution](runbooks.md): setup, first run, offline run, and local no-Airflow execution.
- [Scripts Reference](scripts-reference.md): why each `scripts/` entry exists, when to use it, and the steps it performs.
- [Repository Map & Configuration Guide](project-reference.md): file-level reference and configuration controls.
- [Metadata-Driven Architecture and YAML Patterns](metadata-driven-architecture.md): how YAML pipelines are authored and executed.

Continue with [Framework Overview, System Design & Tech Stack](architecture.md) for technical context or [Runbook and Local Execution](runbooks.md) to run the platform.
