from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import requests
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from risk_analytics.config import load_config
from risk_analytics.spark import create_spark_session
from risk_analytics.yaml_executor import PipelineValidationError, preview_pipeline_yaml, run_pipeline_from_yaml, validate_pipeline_yaml


def _link(name: str, url: str, description: str) -> dict[str, str]:
    return {"name": name, "url": url, "description": description}


LINKS: list[dict[str, str]] = [
    _link("Business Dashboard", os.getenv("BUSINESS_UI_URL", "http://localhost:8501"), "Risk metrics and summary KPIs."),
    _link("Developer Dashboard", os.getenv("DEVELOPER_UI_URL", "http://localhost:8502"), "Pipeline controls and runtime visibility."),
    _link("JupyterLab Notebook", os.getenv("NOTEBOOK_URL", "http://localhost:8888"), "Interactive Spark Connect notebooks."),
    _link("Dremio", os.getenv("DREMIO_URL", "http://localhost:9047"), "SQL exploration on Nessie/Iceberg tables."),
    _link("Nessie Catalog UI", os.getenv("NESSIE_UI_URL", "http://localhost:19120/tree/main"), "Versioned catalog browser for branches, commits, and tables."),
    _link("Airflow", os.getenv("AIRFLOW_URL", "http://localhost:8088"), "DAG orchestration and task monitoring."),
    _link("Kafka UI", os.getenv("KAFKA_UI_URL", "http://localhost:8090"), "Browse Kafka topics, messages, and consumer groups."),
]


API_VERSION = "2.0.0"

app = FastAPI(title="Risk Analytics Platform API", version=API_VERSION)
REPO_ROOT = Path(__file__).resolve().parent.parent
TRANSFORM_DIR = REPO_ROOT / "transform"

ENTITIES = ("customer", "asset", "collateral", "deals")
SOURCE_LABELS = {"sourcea": "sourceA", "sourceb": "sourceB"}
CATALOG_NAMESPACE_KEYS = ("namespace", "stage_namespace", "ods_namespace")
ORCHESTRATION_DAG_ID = "ra_stage_to_ods_orchestration"
BOOTSTRAP_DAG_ID = "ra_createtables_and_data"
RISK_METRICS_DAG_ID = "ra_riskmetrics_eval_ods"


class PipelineRequest(BaseModel):
  pipeline: str = Field(..., description="Pipeline YAML file path or name")
  params: dict[str, Any] = Field(default_factory=dict)


class ExecutePipelineRequest(PipelineRequest):
  spark_ref: str = Field(default="main")
  app_name: str = Field(default="risk-analytics-api-executor")


class PipelineContentUpdate(BaseModel):
  content: str = Field(..., description="Full YAML content to persist")


class SourceToOdsTriggerRequest(BaseModel):
  mode: str = Field(default="full", description="One of: full, stage, ods")
  entity: str = Field(default="customer", description="Entity name for stage/ods mode")
  source: str = Field(default="sourcea", description="One of: sourcea, sourceb")
  as_of_date: str = Field(default_factory=lambda: date.today().isoformat())
  paths: dict[str, str] = Field(default_factory=dict)


class PipelineExecuteRequest(BaseModel):
  target: str = Field(default="orchestration", description="One of: bootstrap, orchestration, stage, ods, riskmetrics")
  entity: str = Field(default="customer", description="Entity name for the stage/ods targets")
  source: str = Field(default="sourcea", description="One of: sourcea, sourceb")
  as_of_date: str = Field(default_factory=lambda: date.today().isoformat())
  data_model: str = Field(default="source-to-ods", description="Data model for the risk metrics target")
  paths: dict[str, str] = Field(default_factory=dict)


class KafkaPublishRequest(BaseModel):
  entity: str = Field(..., description="One of: deals, customer, asset, collateral")
  payload: dict[str, Any] = Field(default_factory=dict)
  topic: str | None = Field(default=None)
  trigger_pipeline: bool = Field(default=False)
  as_of_date: str = Field(default_factory=lambda: date.today().isoformat())


def _resolve_pipeline_path(pipeline: str) -> Path:
  """Resolve a pipeline name inside the controlled transform directory.

  Rejecting path separators prevents the editing API from reading or writing an
  arbitrary repository file; callers may operate only on known YAML definitions.
  """
  if "/" in pipeline or "\\" in pipeline:
    raise FileNotFoundError("Pipeline name must be a file name under transform directory.")
  candidate = Path(pipeline)
  if not candidate.is_absolute():
    candidate = TRANSFORM_DIR / candidate
  if not candidate.exists():
    raise FileNotFoundError(f"Pipeline YAML not found: {candidate}")
  return candidate


def _stage_dag_id(entity: str, source: str) -> str:
  return f"ra_{SOURCE_LABELS[source]}_{entity}_stage"


def _ods_dag_id(entity: str, source: str) -> str:
  return f"ra_{SOURCE_LABELS[source]}_{entity}_ods"


def _validate_entity_and_source(entity: str, source: str) -> tuple[str, str]:
  entity = entity.strip().lower()
  source = source.strip().lower()
  if entity not in ENTITIES:
    raise HTTPException(status_code=400, detail=f"entity must be one of: {', '.join(ENTITIES)}")
  if source not in SOURCE_LABELS:
    raise HTTPException(status_code=400, detail=f"source must be one of: {', '.join(SOURCE_LABELS)}")
  return entity, source


def _trigger_airflow_dag(dag_id: str, conf: dict[str, Any]) -> dict[str, Any]:
  base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
  auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
  payload = {
      "logical_date": f"{conf.get('as_of_date', date.today().isoformat())}T00:00:00Z",
      "conf": conf,
  }
  response = requests.post(f"{base_url.rstrip('/')}/dags/{dag_id}/dagRuns", json=payload, auth=auth, timeout=30)
  response.raise_for_status()
  return response.json()


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    """Render a dependency-free landing page for the local service ecosystem."""
    cards = "".join(
        f"""
        <a class=\"card\" href=\"{item['url']}\" target=\"_blank\" rel=\"noreferrer\">
          <h2>{item['name']}</h2>
          <p>{item['description']}</p>
          <span>{item['url']}</span>
        </a>
        """
        for item in LINKS
    )

    return f"""
    <!doctype html>
    <html lang=\"en\">
      <head>
        <meta charset=\"utf-8\" />
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
        <title>Risk Analytics Links</title>
        <style>
          :root {{
            color-scheme: dark;
            --bg: #08111f;
            --panel: #101c33;
            --panel-2: #152642;
            --text: #e8eefc;
            --muted: #a7b6d9;
            --accent: #76b7ff;
            --border: rgba(118, 183, 255, 0.22);
          }}
          body {{
            margin: 0;
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            background: radial-gradient(circle at top left, #122344 0, var(--bg) 52%);
            color: var(--text);
          }}
          .shell {{ max-width: 1100px; margin: 0 auto; padding: 48px 24px 56px; }}
          .hero {{ margin-bottom: 24px; }}
          .eyebrow {{ text-transform: uppercase; letter-spacing: .18em; color: var(--accent); font-size: .78rem; margin: 0 0 10px; }}
          h1 {{ margin: 0; font-size: clamp(2rem, 4vw, 3.6rem); line-height: 1.05; }}
          .lede {{ max-width: 760px; color: var(--muted); font-size: 1.05rem; line-height: 1.6; }}
          .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-top: 28px; }}
          .card {{ display: block; text-decoration: none; color: inherit; background: linear-gradient(180deg, var(--panel), var(--panel-2)); border: 1px solid var(--border); border-radius: 20px; padding: 20px; transition: transform .15s ease, border-color .15s ease, box-shadow .15s ease; }}
          .card:hover {{ transform: translateY(-3px); border-color: rgba(118, 183, 255, 0.48); box-shadow: 0 20px 44px rgba(0, 0, 0, .26); }}
          .card h2 {{ margin: 0 0 10px; font-size: 1.15rem; }}
          .card p {{ margin: 0 0 16px; color: var(--muted); line-height: 1.5; }}
          .card span {{ display: inline-block; font-size: .92rem; color: var(--accent); word-break: break-all; }}
          .footer {{ margin-top: 28px; color: var(--muted); font-size: .92rem; }}
        </style>
      </head>
      <body>
        <main class=\"shell\">
          <section class=\"hero\">
            <p class=\"eyebrow\">Risk Analytics Portal</p>
            <h1>Open the local services from one place.</h1>
            <p class=\"lede\">This FastAPI landing page collects the main development endpoints for the platform: dashboards, notebooks, Nessie, Dremio, and Airflow.</p>
          </section>
          <section class=\"grid\">{cards}</section>
          <p class=\"footer\">Tip: use the JSON endpoint at <a href=\"/links\" style=\"color: var(--accent)\">/links</a> for automation or external tooling.</p>
        </main>
      </body>
    </html>
    """


@app.get("/links")
def links() -> dict[str, list[dict[str, str]]]:
    return {"links": LINKS}


def _check_nessie() -> dict[str, Any]:
  """Report reachability of the versioned catalog and its default branch."""
  config = load_config()
  uri = config["catalog"]["nessie_uri"].rstrip("/")
  try:
    response = requests.get(f"{uri}/trees/main", timeout=10)
    response.raise_for_status()
  except requests.RequestException as error:
    return {"status": "unavailable", "uri": uri, "error": str(error)}
  reference = response.json().get("reference", {})
  return {"status": "ok", "uri": uri, "default_branch": reference.get("name", "main"), "hash": reference.get("hash")}


def _check_spark() -> dict[str, Any]:
  """Run the cheapest possible query so the check proves an executable session."""
  try:
    spark = create_spark_session("risk-analytics-api-health")
  except Exception as error:  # pragma: no cover - environment dependent
    return {"status": "unavailable", "error": str(error)}
  try:
    spark.sql("SELECT 1").collect()
    return {"status": "ok", "remote": os.getenv("SPARK_REMOTE", "local")}
  except Exception as error:  # pragma: no cover - environment dependent
    return {"status": "unavailable", "error": str(error)}
  finally:
    try:
      spark.stop()
    except Exception:  # pragma: no cover - stop is best effort
      pass


@app.get("/health")
def health(include_spark: bool = True) -> dict[str, Any]:
  """Report API, Spark, and Nessie catalog status.

  Component failures are reported in the payload instead of raising, so a probe
  can distinguish a degraded platform from an unreachable API.
  """
  components: dict[str, Any] = {"api": {"status": "ok", "version": API_VERSION}}
  components["nessie"] = _check_nessie()
  components["spark"] = _check_spark() if include_spark else {"status": "skipped"}

  degraded = [name for name, state in components.items() if state.get("status") == "unavailable"]
  return {
    "status": "degraded" if degraded else "ok",
    "degraded_components": degraded,
    "components": components,
  }


@app.get("/tables")
def tables() -> dict[str, Any]:
  """List the Iceberg tables registered in the Nessie catalog namespaces."""
  config = load_config()
  catalog_name = config["catalog"].get("name", "nessie")
  namespaces = [config["catalog"][key] for key in CATALOG_NAMESPACE_KEYS if config["catalog"].get(key)]

  try:
    spark = create_spark_session("risk-analytics-api-tables")
  except Exception as error:
    raise HTTPException(status_code=503, detail=f"Spark session unavailable: {error}") from error

  discovered: list[dict[str, str]] = []
  errors: dict[str, str] = {}
  try:
    for namespace in namespaces:
      try:
        rows = spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace}").collect()
      except Exception as error:
        errors[namespace] = str(error)
        continue
      for row in rows:
        table_name = row["tableName"]
        discovered.append(
          {
            "namespace": namespace,
            "table": table_name,
            "identifier": f"{catalog_name}.{namespace}.{table_name}",
          }
        )
  finally:
    try:
      spark.stop()
    except Exception:  # pragma: no cover - stop is best effort
      pass

  return {
    "catalog": catalog_name,
    "namespaces": namespaces,
    "table_count": len(discovered),
    "tables": sorted(discovered, key=lambda item: item["identifier"]),
    "unreadable_namespaces": errors,
  }


@app.post("/pipeline/execute")
def execute_pipeline_run(request: PipelineExecuteRequest) -> dict[str, Any]:
  """Trigger a pipeline execution through Airflow.

  ``target`` selects the DAG so callers can run the whole STAGE/ODS chain or a
  single namespace step without knowing the DAG naming convention.
  """
  target = request.target.strip().lower()
  conf: dict[str, Any] = {"as_of_date": request.as_of_date, **request.paths}

  if target == "bootstrap":
    dag_id = BOOTSTRAP_DAG_ID
  elif target == "orchestration":
    dag_id = ORCHESTRATION_DAG_ID
  elif target == "riskmetrics":
    dag_id = RISK_METRICS_DAG_ID
    conf["data_model"] = request.data_model
  elif target in {"stage", "ods"}:
    entity, source = _validate_entity_and_source(request.entity, request.source)
    dag_id = _stage_dag_id(entity, source) if target == "stage" else _ods_dag_id(entity, source)
    conf.update({"entity": entity, "source": SOURCE_LABELS[source]})
  else:
    raise HTTPException(
      status_code=400,
      detail="target must be one of: bootstrap, orchestration, stage, ods, riskmetrics",
    )

  try:
    result = _trigger_airflow_dag(dag_id, conf)
  except requests.RequestException as error:
    raise HTTPException(status_code=502, detail=f"Airflow trigger failed: {error}") from error

  return {
    "status": "triggered",
    "target": target,
    "dag_id": dag_id,
    "dag_run_id": result.get("dag_run_id"),
    "conf": conf,
  }


@app.get("/api/v1/pipelines")
def list_pipelines() -> dict[str, list[str]]:
  pipelines = sorted(path.name for path in TRANSFORM_DIR.glob("*.yaml"))
  return {"pipelines": pipelines}


@app.get("/api/v1/pipelines/{pipeline_name}")
def get_pipeline(pipeline_name: str) -> dict[str, Any]:
  try:
    pipeline_path = _resolve_pipeline_path(pipeline_name)
    content = pipeline_path.read_text(encoding="utf-8")
  except FileNotFoundError as error:
    raise HTTPException(status_code=404, detail=str(error)) from error
  return {"pipeline": pipeline_path.name, "content": content}


@app.put("/api/v1/pipelines/{pipeline_name}")
def update_pipeline(pipeline_name: str, request: PipelineContentUpdate) -> dict[str, Any]:
  """Validate YAML before persisting a pipeline definition through the API."""
  try:
    pipeline_path = _resolve_pipeline_path(pipeline_name)
    yaml.safe_load(request.content)
    pipeline_path.write_text(request.content, encoding="utf-8")
  except FileNotFoundError as error:
    raise HTTPException(status_code=404, detail=str(error)) from error
  except yaml.YAMLError as error:
    raise HTTPException(status_code=400, detail=f"Invalid YAML: {error}") from error
  return {"status": "updated", "pipeline": pipeline_path.name}


@app.post("/api/v1/pipelines/validate")
def validate_pipeline(request: PipelineRequest) -> dict[str, Any]:
  try:
    pipeline_path = _resolve_pipeline_path(request.pipeline)
    summary = validate_pipeline_yaml(pipeline_path=pipeline_path, runtime_params=request.params)
  except (PipelineValidationError, FileNotFoundError, ValueError) as error:
    return {"valid": False, "error": str(error)}
  return {"valid": True, "pipeline": pipeline_path.name, "summary": summary}


@app.post("/api/v1/pipelines/preview")
def preview_pipeline(request: PipelineRequest) -> dict[str, Any]:
  try:
    pipeline_path = _resolve_pipeline_path(request.pipeline)
    preview = preview_pipeline_yaml(pipeline_path=pipeline_path, runtime_params=request.params)
  except (PipelineValidationError, FileNotFoundError, ValueError) as error:
    return {"ok": False, "error": str(error)}
  return {
    "ok": True,
    "pipeline": pipeline_path.name,
    "summary": preview["summary"],
    "rendered": preview["rendered"],
  }


@app.post("/api/v1/pipelines/execute")
def execute_pipeline(request: ExecutePipelineRequest) -> dict[str, Any]:
  """Execute a selected YAML definition and return compact write statistics.

  Spark is created per request and always stopped in ``finally`` so interactive
  API use cannot leave orphaned sessions after an execution error.
  """
  try:
    pipeline_path = _resolve_pipeline_path(request.pipeline)
    config = load_config()
    # Provide a sensible reporting date while allowing an explicit request value
    # to override it as part of the reusable pipeline contract.
    runtime_params = {
      "as_of_date": request.params.get("as_of_date", date.today().isoformat()),
      **request.params,
    }
    spark = create_spark_session(request.app_name, ref=request.spark_ref)
    try:
      result = run_pipeline_from_yaml(
        spark=spark,
        pipeline_path=pipeline_path,
        config=config,
        runtime_params=runtime_params,
      )
    finally:
      spark.stop()
  except (PipelineValidationError, FileNotFoundError, ValueError) as error:
    return {"status": "failed", "error": str(error)}
  except Exception as error:  # pragma: no cover - defensive runtime envelope
    return {"status": "failed", "error": f"Pipeline execution error: {error}"}

  return {
    "status": "success",
    "pipeline": pipeline_path.name,
    "target_row_counts": result.target_row_counts,
  }


@app.post("/api/v1/process/source-to-ods/trigger")
def trigger_source_to_ods(request: SourceToOdsTriggerRequest) -> dict[str, Any]:
  mode = request.mode.strip().lower()
  if mode not in {"full", "stage", "ods"}:
    raise HTTPException(status_code=400, detail="mode must be one of: full, stage, ods")

  entity = request.entity.strip().lower()
  source = request.source.strip().lower()
  if entity not in {"customer", "asset", "collateral", "deals"}:
    raise HTTPException(status_code=400, detail="entity must be one of: customer, asset, collateral, deals")
  if source not in {"sourcea", "sourceb"}:
    raise HTTPException(status_code=400, detail="source must be one of: sourcea, sourceb")

  if mode == "full":
    dag_id = ORCHESTRATION_DAG_ID
  else:
    dag_id = _stage_dag_id(entity, source) if mode == "stage" else _ods_dag_id(entity, source)
  conf = {
    "as_of_date": request.as_of_date,
    "entity": entity,
    "source": source,
    **request.paths,
  }

  try:
    result = _trigger_airflow_dag(dag_id, conf)
  except requests.RequestException as error:
    raise HTTPException(status_code=502, detail=f"Airflow trigger failed: {error}") from error

  return {
    "status": "triggered",
    "dag_id": dag_id,
    "dag_run_id": result.get("dag_run_id"),
    "conf": conf,
  }


@app.post("/api/v1/kafka/publish")
def publish_kafka_event(request: KafkaPublishRequest) -> dict[str, Any]:
  entity = request.entity.strip().lower()
  topic_map = {
    "deals": "risk.deals.ingest",
    "customer": "risk.customer.ingest",
    "asset": "risk.asset.ingest",
    "collateral": "risk.collateral.ingest",
  }
  if entity not in topic_map:
    raise HTTPException(status_code=400, detail="entity must be one of: deals, customer, asset, collateral")

  topic = request.topic or topic_map[entity]
  payload = {
    "entity": entity,
    "as_of_date": request.as_of_date,
    **request.payload,
  }

  try:
    from confluent_kafka import Producer
  except Exception as error:  # pragma: no cover - dependency guard for local runs
    raise HTTPException(status_code=500, detail=f"Kafka producer dependency unavailable: {error}") from error

  producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")})
  try:
    producer.produce(topic, value=json.dumps(payload).encode("utf-8"))
    producer.flush(5)
  except Exception as error:
    raise HTTPException(status_code=502, detail=f"Kafka publish failed: {error}") from error

  triggered = None
  if request.trigger_pipeline:
    pipeline_conf = {"as_of_date": request.as_of_date}
    try:
      triggered = _trigger_airflow_dag(RISK_METRICS_DAG_ID, pipeline_conf)
    except requests.RequestException as error:
      raise HTTPException(status_code=502, detail=f"Kafka publish succeeded, but pipeline trigger failed: {error}") from error

  return {
    "status": "published",
    "topic": topic,
    "payload": payload,
    "pipeline_triggered": bool(triggered),
    "pipeline_dag_run_id": triggered.get("dag_run_id") if triggered else None,
  }
