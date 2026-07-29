from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel, Field
from fastapi.responses import HTMLResponse
import requests
import yaml

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
    _link("Nessie API", os.getenv("NESSIE_URL", "http://localhost:19120/api/v2"), "Versioned catalog API and branch operations."),
    _link("Airflow", os.getenv("AIRFLOW_URL", "http://localhost:8088"), "DAG orchestration and task monitoring."),
    _link("Kafka UI", os.getenv("KAFKA_UI_URL", "http://localhost:8090"), "Browse Kafka topics, messages, and consumer groups."),
]


app = FastAPI(title="Risk Analytics Links", version="1.0.0")
REPO_ROOT = Path(__file__).resolve().parent.parent
TRANSFORM_DIR = REPO_ROOT / "transform"


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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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

  dag_id_map = {
    "full": "risk_analytics_source_to_ods_orchestration",
    "stage": "risk_analytics_stage_load",
    "ods": "risk_analytics_ods_load",
  }
  dag_id = dag_id_map[mode]
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
      triggered = _trigger_airflow_dag("risk_analytics_kafka_entity_orchestration", pipeline_conf)
    except requests.RequestException as error:
      raise HTTPException(status_code=502, detail=f"Kafka publish succeeded, but pipeline trigger failed: {error}") from error

  return {
    "status": "published",
    "topic": topic,
    "payload": payload,
    "pipeline_triggered": bool(triggered),
    "pipeline_dag_run_id": triggered.get("dag_run_id") if triggered else None,
  }
