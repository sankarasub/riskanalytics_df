# Production Deployment Guide

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Runbook](runbooks.md) | [File Guide](project-reference.md) | [Dependency Guide](dependency-cache-guide.md) | [Troubleshooting](troubleshooting.md)

## Production Delivery Model

The local Compose environment is a complete integration demonstration, not a production topology. A production deployment separates stateful services, persists catalog and orchestration metadata, manages secrets outside source control, and deploys immutable images through a CI/CD pipeline.

The migration principle is consistent across environments: retain the logical contracts—Spark processing, Iceberg tables, catalog references, object storage, Airflow orchestration, Kafka events, and the developer experience—while replacing local containers with managed or platform-operated equivalents.

## Required Production Controls

- Build and publish immutable images with version tags and image digests; promote the same artifact between environments.
- Store credentials in a secret manager and inject them through workload identity or secret references. Never retain local development defaults in production.
- Use durable catalog, object-store, Airflow metadata, Kafka, and application data stores with backup, retention, and recovery policies.
- Run Spark workloads with resource quotas, autoscaling policies, structured logs, metrics, alerting, and centralized audit trails.
- Place user-facing applications behind TLS, identity-aware access control, network policies, and an ingress or load balancer.
- Maintain separate development, test, and production catalog references, warehouses, topics, and credentials.

## Platform Equivalents

| Local capability | On-premises Kubernetes | AWS | Azure | GCP |
| --- | --- | --- | --- | --- |
| Spark | Spark Operator or managed distribution | EMR on EKS / EMR Serverless | Azure Databricks | Dataproc Serverless / Dataproc on GKE |
| Iceberg warehouse | S3-compatible object store | Amazon S3 | ADLS Gen2 | Cloud Storage |
| Versioned catalog | Nessie on Kubernetes with durable backing store | Nessie on EKS/ECS or governed catalog strategy | Nessie on AKS or Databricks Unity Catalog strategy | Nessie on GKE or BigLake-compatible governance strategy |
| Airflow | Airflow on Kubernetes / Astronomer | MWAA | Managed Airflow on AKS or Azure Data Factory orchestration | Cloud Composer |
| Kafka event bus | Strimzi / Confluent Platform | Amazon MSK / Confluent Cloud | Event Hubs with Kafka endpoint / Confluent Cloud | Pub/Sub or Confluent Cloud |
| Streamlit and API | Kubernetes Deployments + Ingress | ECS/Fargate, EKS, or App Runner | AKS or Container Apps | GKE or Cloud Run |
| Observability | Prometheus, Grafana, Loki | CloudWatch, X-Ray, OpenSearch | Azure Monitor, Log Analytics | Cloud Monitoring, Cloud Logging |

## Deployment Strategies

### On-premises Kubernetes

Deploy each stateless application as a Kubernetes Deployment and manage Spark with the Spark Operator. Run Airflow with a production-grade executor, such as KubernetesExecutor or CeleryExecutor. Use a highly available object store and external PostgreSQL for Airflow and Nessie metadata. Kafka should run through an operator such as Strimzi or an enterprise Kafka distribution. Ingress, cert-manager, network policies, and an enterprise identity provider complete the perimeter.

### AWS

Use S3 for the Iceberg warehouse, MSK for Kafka, MWAA for Airflow, and EMR Serverless or EMR on EKS for Spark. Host the UI and API on ECS/Fargate, EKS, or App Runner. Store credentials and connection details in Secrets Manager, use IAM roles for service access, and collect logs and metrics with CloudWatch. Nessie can run on EKS/ECS with Aurora PostgreSQL for durable metadata when its branch-based catalog workflow is retained.

### Azure

Use ADLS Gen2 for the warehouse, Event Hubs’ Kafka-compatible endpoint or Confluent Cloud for events, and Databricks for managed Spark. Orchestrate through Azure Data Factory or a managed Airflow deployment on AKS. Host the UI and API in Container Apps or AKS; use Key Vault and managed identities for access. Azure Monitor provides the primary operational view.

### GCP

Use Cloud Storage for table files, Dataproc Serverless for Spark, Cloud Composer for Airflow, and Pub/Sub for eventing. When the Kafka protocol itself is required, use Confluent Cloud or Kafka on GKE. Deploy Streamlit and FastAPI to Cloud Run or GKE. Secret Manager, Cloud Monitoring, and Cloud Logging provide platform controls.

## Offline and Artifact Promotion

The project’s `offline` launcher is intentionally strict: it starts only cached images and performs neither builds nor pulls. Establish the cache through a connected build, then distribute the resulting images through a private registry, image archive, or approved artifact repository. Production delivery should never build images on the runtime host; CI builds, scans, signs, and publishes them before deployment.
