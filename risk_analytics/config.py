from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def fq_table_name(config: dict[str, Any], namespace_key: str, table_name: str) -> str:
    """Build a fully-qualified Iceberg table name from catalog config."""
    catalog = config.get("catalog", {})
    catalog_name = catalog.get("name", "nessie")
    namespace = catalog.get(namespace_key)
    if not namespace:
        raise ValueError(f"Missing catalog namespace key '{namespace_key}' in configuration.")
    return f"{catalog_name}.{namespace}.{table_name}"


def legacy_table_name(config: dict[str, Any], table_name: str) -> str:
    return fq_table_name(config, "namespace", table_name)


def stage_table_name(config: dict[str, Any], table_name: str) -> str:
    return fq_table_name(config, "stage_namespace", table_name)


def ods_table_name(config: dict[str, Any], table_name: str) -> str:
    return fq_table_name(config, "ods_namespace", table_name)


def _resolve_pipeline_path(pipeline_path: str) -> str:
    """Return an absolute pipeline path anchored at the repository root."""
    candidate = Path(pipeline_path)
    if candidate.is_absolute():
        return candidate.as_posix()
    return (REPO_ROOT / candidate).resolve().as_posix()


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(mode: str = None) -> dict[str, Any]:
    """Load the platform contract with execution mode support.

    Args:
        mode: Execution mode (docker, hybrid, local). Defaults to EXECUTION_MODE env var or 'docker'.

    Defaults remain in version-controlled YAML while container or host-specific
    addresses are supplied through environment variables. This keeps source code
    portable between Docker services and local execution contexts.
    """
    mode = mode or os.getenv("EXECUTION_MODE", "docker")
    
    # Load base configuration
    path = REPO_ROOT / "config" / "platform.yaml"
    with path.open(encoding="utf-8") as source:
        config = yaml.safe_load(source)
    
    # Apply mode-specific overrides
    mode_path = REPO_ROOT / "config" / "modes" / f"{mode}.yaml"
    if mode_path.exists():
        with mode_path.open(encoding="utf-8") as source:
            mode_config = yaml.safe_load(source)
            config = _deep_merge(config, mode_config)
    
    # Set default namespaces if not present
    catalog = config.setdefault("catalog", {})
    catalog.setdefault("namespace", "risk_analytics")
    catalog.setdefault("stage_namespace", "risk_analytics_stage")
    catalog.setdefault("ods_namespace", "risk_analytics_ods")
    
    # Apply environment variable overrides
    if "nessie_uri" in catalog:
        config["catalog"]["nessie_uri"] = os.getenv("NESSIE_URI", catalog["nessie_uri"])
    if "endpoint" in config.get("storage", {}):
        config["storage"]["endpoint"] = os.getenv("S3_ENDPOINT", config["storage"]["endpoint"])
    
    # Resolve pipeline path
    config.setdefault("executor", {})
    pipeline_path = os.getenv(
        "RISK_PIPELINE_YAML",
        config["executor"].get("pipeline_path", "transform/risk_metrics_pipeline.yaml"),
    )
    config["executor"]["pipeline_path"] = _resolve_pipeline_path(pipeline_path)
    
    # Store execution mode in config for reference
    config["execution_mode"] = mode
    
    return config
