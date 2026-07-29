"""UI-facing infrastructure adapters kept separate from presentation code."""
from __future__ import annotations

import os
import subprocess
import requests
from risk_analytics.spark import create_spark_session

BOOTSTRAP_DAG_ID = "ra_createtables_and_data"
ORCHESTRATION_DAG_ID = "ra_stage_to_ods_orchestration"
RISK_METRICS_DAG_ID = "ra_riskmetrics_eval_ods"
KAFKA_STAGE_DAG_ID = "ra_kafka_customer_stage"

# Compose service name of the FastAPI container.
DEFAULT_PIPELINE_API_URL = "http://links-api:8000/api/v1"


def pipeline_api_url() -> str:
    return os.getenv("PIPELINE_API_URL", DEFAULT_PIPELINE_API_URL).rstrip("/")


def read_risk_metrics():
    """Read the published metrics with customer names for the business dashboard.

    Presentation code receives a ready-to-display Pandas frame, while this adapter
    owns Spark lifecycle and lakehouse access. That keeps UI files focused on
    interaction rather than distributed-compute concerns.
    """
    spark = create_spark_session("risk-analytics-business-ui")
    try:
        try:
            metrics = spark.table("nessie.risk_analytics_ods.risk_metrics")
            customers = spark.table("nessie.risk_analytics_ods.customer").select(
                "customer_id", "as_of_date", "customer_name"
            )
        except Exception:
            metrics = spark.table("nessie.risk_analytics.risk_metrics")
            customers = spark.table("nessie.risk_analytics.customer_canonical").select(
                "customer_id", "as_of_date", "customer_name"
            )
        return metrics.join(customers, ["customer_id", "as_of_date"], "left").toPandas()
    finally:
        spark.stop()


def nessie_ui_url() -> str:
    """Browser URL for the Nessie catalog UI (distinct from the REST API URI)."""
    return os.getenv("NESSIE_UI_URL", "http://localhost:19120/tree/main")


def nessie_references() -> list[dict]:
    uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
    response = requests.get(f"{uri.rstrip('/')}/trees", timeout=10)
    response.raise_for_status()
    return response.json().get("references", [])


def trigger_airflow_dag(dag_id: str, logical_date: str | None = None) -> dict:
    """Start a named Airflow DAG through its authenticated REST interface."""
    base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
    auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
    body = {"logical_date": logical_date} if logical_date else {}
    response = requests.post(f"{base_url}/dags/{dag_id}/dagRuns", json=body, auth=auth, timeout=20)
    response.raise_for_status()
    return response.json()


def list_transform_pipelines() -> list[str]:
    api_url = pipeline_api_url()
    response = requests.get(f"{api_url}/pipelines", timeout=20)
    response.raise_for_status()
    return response.json().get("pipelines", [])


def validate_transform_pipeline(pipeline: str, params: dict) -> dict:
    api_url = pipeline_api_url()
    response = requests.post(
        f"{api_url}/pipelines/validate",
        json={"pipeline": pipeline, "params": params},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def preview_transform_pipeline(pipeline: str, params: dict) -> dict:
    api_url = pipeline_api_url()
    response = requests.post(
        f"{api_url}/pipelines/preview",
        json={"pipeline": pipeline, "params": params},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def execute_transform_pipeline(pipeline: str, params: dict, spark_ref: str = "main") -> dict:
    """Request an API-managed Spark execution with a long-running timeout.

    A transform may submit distributed Spark work, so this intentionally permits
    a longer request window than the lightweight list, preview, and validate calls.
    """
    api_url = pipeline_api_url()
    response = requests.post(
        f"{api_url}/pipelines/execute",
        json={"pipeline": pipeline, "params": params, "spark_ref": spark_ref},
        timeout=3600,
    )
    response.raise_for_status()
    return response.json()


def load_pipeline_yaml_text(pipeline: str) -> str:
    api_url = pipeline_api_url()
    response = requests.get(f"{api_url}/pipelines/{pipeline}", timeout=20)
    response.raise_for_status()
    return response.json().get("content", "")


def save_pipeline_yaml_text(pipeline: str, content: str) -> None:
    api_url = pipeline_api_url()
    response = requests.put(
        f"{api_url}/pipelines/{pipeline}",
        json={"content": content},
        timeout=30,
    )
    response.raise_for_status()


def trigger_source_to_ods(mode: str, entity: str, source: str, as_of_date: str, paths: dict[str, str] | None = None) -> dict:
    api_url = pipeline_api_url()
    response = requests.post(
        f"{api_url}/process/source-to-ods/trigger",
        json={
            "mode": mode,
            "entity": entity,
            "source": source,
            "as_of_date": as_of_date,
            "paths": paths or {},
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def publish_kafka_event(entity: str, payload: dict, as_of_date: str, trigger_pipeline: bool = False, topic: str | None = None) -> dict:
    api_url = pipeline_api_url()
    response = requests.post(
        f"{api_url}/kafka/publish",
        json={
            "entity": entity,
            "payload": payload,
            "as_of_date": as_of_date,
            "trigger_pipeline": trigger_pipeline,
            "topic": topic,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def get_docker_platform_status() -> dict:
    """Get Docker Compose platform status."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            return {"status": "error", "message": result.stderr}
        
        import json
        services = json.loads(result.stdout)
        return {
            "status": "success",
            "services": services,
            "total": len(services),
            "running": sum(1 for s in services if s.get("State") == "running"),
        }
    except Exception as error:
        return {"status": "error", "message": str(error)}


def start_docker_platform(mode: str = "up") -> dict:
    """Start Docker Compose platform."""
    try:
        if mode == "build":
            result = subprocess.run(
                ["docker", "compose", "up", "--build", "-d"],
                capture_output=True,
                text=True,
                timeout=300,
            )
        else:
            result = subprocess.run(
                ["docker", "compose", "up", "-d"],
                capture_output=True,
                text=True,
                timeout=180,
            )
        
        if result.returncode != 0:
            return {"status": "error", "message": result.stderr}
        
        return {"status": "success", "message": result.stdout}
    except Exception as error:
        return {"status": "error", "message": str(error)}


def stop_docker_platform() -> dict:
    """Stop Docker Compose platform."""
    try:
        result = subprocess.run(
            ["docker", "compose", "down"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        
        if result.returncode != 0:
            return {"status": "error", "message": result.stderr}
        
        return {"status": "success", "message": result.stdout}
    except Exception as error:
        return {"status": "error", "message": str(error)}


def list_airflow_dags() -> list[dict]:
    """List all available Airflow DAGs."""
    try:
        base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
        auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
        response = requests.get(f"{base_url}/dags", auth=auth, timeout=20)
        response.raise_for_status()
        return response.json().get("dags", [])
    except Exception as error:
        return []


def get_airflow_dag_runs(dag_id: str, limit: int = 10) -> list[dict]:
    """Get recent DAG runs for a specific DAG."""
    try:
        base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
        auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
        response = requests.get(
            f"{base_url}/dags/{dag_id}/dagRuns",
            auth=auth,
            params={"limit": limit},
            timeout=20,
        )
        response.raise_for_status()
        return response.json().get("dag_runs", [])
    except Exception as error:
        return []


def get_airflow_dag_status(dag_id: str) -> dict:
    """Get current status of a DAG including latest run info."""
    try:
        base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
        auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
        
        # Get DAG info
        response = requests.get(f"{base_url}/dags/{dag_id}", auth=auth, timeout=20)
        response.raise_for_status()
        dag_info = response.json()
        
        # Get latest runs
        runs = get_airflow_dag_runs(dag_id, limit=1)
        latest_run = runs[0] if runs else None
        
        return {
            "dag_id": dag_id,
            "is_active": dag_info.get("is_active", False),
            "is_paused": dag_info.get("is_paused", False),
            "last_parsed": dag_info.get("last_parsed"),
            "latest_run": latest_run,
        }
    except Exception as error:
        return {"dag_id": dag_id, "error": str(error)}


def get_table_counts(as_of_date: str | None = None) -> dict:
    """Get row counts for all key tables in the lakehouse."""
    spark = create_spark_session("risk-analytics-table-counts")
    try:
        counts = {}
        
        # Try ODS tables first (new data model)
        ods_tables = [
            "nessie.risk_analytics_ods.customer",
            "nessie.risk_analytics_ods.asset",
            "nessie.risk_analytics_ods.collateral",
            "nessie.risk_analytics_ods.deals",
            "nessie.risk_analytics_ods.risk_metrics",
        ]
        
        for table in ods_tables:
            try:
                df = spark.table(table)
                if as_of_date and "as_of_date" in df.columns:
                    count = df.filter(df.as_of_date == as_of_date).count()
                else:
                    count = df.count()
                counts[table] = count
            except Exception:
                # Try legacy tables if ODS not available
                legacy_map = {
                    "nessie.risk_analytics_ods.customer": "nessie.risk_analytics.customer_canonical",
                    "nessie.risk_analytics_ods.asset": "nessie.risk_analytics.asset_canonical",
                    "nessie.risk_analytics_ods.collateral": "nessie.risk_analytics.collateral_canonical",
                    "nessie.risk_analytics_ods.deals": "nessie.risk_analytics.deals_canonical",
                    "nessie.risk_analytics_ods.risk_metrics": "nessie.risk_analytics.risk_metrics",
                }
                legacy_table = legacy_map.get(table)
                if legacy_table:
                    try:
                        df = spark.table(legacy_table)
                        if as_of_date and "as_of_date" in df.columns:
                            count = df.filter(df.as_of_date == as_of_date).count()
                        else:
                            count = df.count()
                        counts[table] = count
                    except Exception:
                        counts[table] = 0
        
        return {"status": "success", "counts": counts}
    except Exception as error:
        return {"status": "error", "message": str(error)}
    finally:
        spark.stop()


def get_table_preview(table_name: str, limit: int = 20) -> dict:
    """Get preview data from a specific table."""
    spark = create_spark_session("risk-analytics-table-preview")
    try:
        df = spark.table(table_name)
        preview = df.limit(limit).toPandas()
        return {
            "status": "success",
            "table": table_name,
            "columns": list(df.columns),
            "row_count": df.count(),
            "preview": preview,
        }
    except Exception as error:
        return {"status": "error", "message": str(error)}
    finally:
        spark.stop()


def get_kafka_topics() -> list:
    """Get list of Kafka topics."""
    try:
        kafka_ui_url = os.getenv("KAFKA_UI_URL", "http://kafka-ui:8080")
        response = requests.get(f"{kafka_ui_url}/api/clusters", timeout=10)
        response.raise_for_status()
        clusters = response.json()
        
        if not clusters:
            return []
        
        cluster_id = clusters[0]["id"]
        topics_response = requests.get(
            f"{kafka_ui_url}/api/clusters/{cluster_id}/topics",
            timeout=10,
        )
        topics_response.raise_for_status()
        return topics_response.json()
    except Exception as error:
        return []


def get_kafka_topic_stats(topic_name: str) -> dict:
    """Get statistics for a specific Kafka topic."""
    try:
        kafka_ui_url = os.getenv("KAFKA_UI_URL", "http://kafka-ui:8080")
        response = requests.get(f"{kafka_ui_url}/api/clusters", timeout=10)
        response.raise_for_status()
        clusters = response.json()
        
        if not clusters:
            return {"error": "No Kafka clusters found"}
        
        cluster_id = clusters[0]["id"]
        topic_response = requests.get(
            f"{kafka_ui_url}/api/clusters/{cluster_id}/topics/{topic_name}",
            timeout=10,
        )
        topic_response.raise_for_status()
        return topic_response.json()
    except Exception as error:
        return {"error": str(error)}


def get_risk_run_history(limit: int = 10) -> list[dict]:
    """Get historical risk metrics run information."""
    spark = create_spark_session("risk-analytics-run-history")
    try:
        try:
            metrics = spark.table("nessie.risk_analytics_ods.risk_metrics")
        except Exception:
            metrics = spark.table("nessie.risk_analytics.risk_metrics")
        
        # Get unique run information
        run_info = metrics.select(
            "risk_run_id", "as_of_date", "calculation_timestamp", "source_branch"
        ).distinct().orderBy("calculation_timestamp", ascending=False).limit(limit).toPandas()
        
        # Get counts per run
        history = []
        for _, row in run_info.iterrows():
            run_id = row["risk_run_id"]
            as_of_date = row["as_of_date"]
            run_metrics = metrics.filter(
                (metrics.risk_run_id == run_id) & (metrics.as_of_date == as_of_date)
            )
            
            history.append({
                "risk_run_id": run_id,
                "as_of_date": str(as_of_date),
                "calculation_timestamp": str(row["calculation_timestamp"]),
                "source_branch": row["source_branch"],
                "record_count": run_metrics.count(),
                "total_pfe": run_metrics.agg({"pfe": "sum"}).collect()[0][0],
                "total_var": run_metrics.agg({"var": "sum"}).collect()[0][0],
            })
        
        return history
    except Exception as error:
        return []
    finally:
        spark.stop()


def trigger_bootstrap_dag(as_of_date: str) -> dict:
    """Trigger the bootstrap DAG to create tables and load seed data."""
    return trigger_airflow_dag(BOOTSTRAP_DAG_ID, f"{as_of_date}T00:00:00Z")


def trigger_risk_metrics_dag(as_of_date: str, data_model: str = "source-to-ods") -> dict:
    """Trigger the risk metrics calculation DAG."""
    base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
    auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
    body = {"logical_date": f"{as_of_date}T00:00:00Z", "conf": {"data_model": data_model}}
    response = requests.post(
        f"{base_url}/dags/{RISK_METRICS_DAG_ID}/dagRuns",
        json=body,
        auth=auth,
        timeout=20,
    )
    response.raise_for_status()
    return response.json()

