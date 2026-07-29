"""Shared naming, templated parameters, and command builders for Risk Analytics DAGs.

DAG identifiers follow the ``ra_<source>_<entity>_<layer>`` convention so the
STAGE and ODS namespaces stay visually separated in the Airflow UI, while the
Spark entry points they call remain generic and parameter driven.
"""
from __future__ import annotations

DEFAULT_ARGS = {"owner": "risk_analytics-platform", "retries": 1}

AS_OF_DATE = "{{ dag_run.conf.get('as_of_date', ds) if dag_run else ds }}"

ENTITIES = ("customer", "asset", "collateral", "deals")

# DAG identifiers use the business-facing source labels; the Spark jobs and YAML
# definitions use the lowercase source keys.
SOURCE_LABELS = {"sourceA": "sourcea", "sourceB": "sourceb"}

SOURCEB_PATH_DEFAULTS = {
    "customer_sourceb_path": "/opt/risk_analytics/data/sourceb/customer/*.csv",
    "asset_sourceb_path": "/opt/risk_analytics/data/sourceb/asset/*.json",
    "product_sourceb_path": "/opt/risk_analytics/data/sourceb/product/*.json",
    "trans_sourceb_path": "/opt/risk_analytics/data/sourceb/trans/*.csv",
    "collateral_sourceb_path": "/opt/risk_analytics/data/sourceb/collateral/*.json",
}

STEP_JOB = "/opt/risk_analytics/jobs/run_source_to_ods_step.py"
BOOTSTRAP_JOB = "/opt/risk_analytics/jobs/bootstrap.py"
RISK_METRICS_JOB = "/opt/risk_analytics/jobs/run_risk_pipeline.py"

SPARK_SUBMIT = "env -u SPARK_REMOTE spark-submit --master local[*]"


def templated_conf_value(key: str, default: str) -> str:
    """Return a Jinja expression that reads ``key`` from the run conf."""
    return f"{{{{ dag_run.conf.get('{key}', '{default}') if dag_run else '{default}' }}}}"


def sourceb_path_conf() -> dict[str, str]:
    """Templated SourceB input paths passed between orchestration and leaf DAGs."""
    return {key: templated_conf_value(key, default) for key, default in SOURCEB_PATH_DEFAULTS.items()}


def stage_dag_id(source_label: str, entity: str) -> str:
    return f"ra_{source_label}_{entity}_stage"


def ods_dag_id(source_label: str, entity: str) -> str:
    return f"ra_{source_label}_{entity}_ods"


def step_command(layer: str, entity: str, source: str, as_of_date: str = AS_OF_DATE) -> str:
    """Build the spark-submit command that runs one YAML-driven layer step."""
    params = " ".join(
        f'--param "{key}={value}"' for key, value in sourceb_path_conf().items()
    )
    return (
        f"{SPARK_SUBMIT} {STEP_JOB} "
        f"--layer {layer} --entity {entity} --source {source} --as-of-date {as_of_date} "
        f"{params}"
    )


def bootstrap_command(action: str, as_of_date: str = AS_OF_DATE, table_name: str | None = None) -> str:
    command = f"{SPARK_SUBMIT} {BOOTSTRAP_JOB} --action {action} --as-of-date {as_of_date}"
    if table_name:
        command = f"{command} --table {table_name}"
    return command


def risk_metrics_command(as_of_date: str = AS_OF_DATE, data_model: str = "source-to-ods") -> str:
    return (
        f"{SPARK_SUBMIT} {RISK_METRICS_JOB} "
        f"--as-of-date {as_of_date} --run-id {{{{ run_id }}}} --data-model {data_model}"
    )
