"""Spark session construction for the Nessie-backed Iceberg catalog."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from risk_analytics.config import load_config


def create_spark_session(app_name: str, ref: str = "main", mode: str = None) -> SparkSession:
    """Create the project Spark entry point for remote notebooks or batch jobs.

    Args:
        app_name: Application name for the Spark session
        ref: Catalog reference (branch name for Nessie)
        mode: Execution mode (docker, hybrid, local). Defaults to EXECUTION_MODE env var.

    ``SPARK_REMOTE`` selects Spark Connect for interactive clients. Batch jobs
    receive the full Iceberg, Nessie, and S3A configuration so every writer uses
    the requested catalog reference and the same warehouse contract.
    """
    cfg = load_config(mode)
    execution_mode = cfg.get("execution_mode", "docker")
    spark_mode = cfg.get("spark_mode", "cluster")
    
    # Use Spark Connect only when explicitly requested by environment.
    # Batch spark-submit jobs unset SPARK_REMOTE and should build a classic
    # SparkSession with master-driven configuration.
    remote = os.getenv("SPARK_REMOTE")

    if remote:
        # Spark Connect already owns server-side catalog configuration.
        return SparkSession.builder.appName(app_name).remote(remote).getOrCreate()

    # Local Spark for local mode
    if spark_mode == "local":
        return _create_local_spark_session(cfg, app_name, ref, execution_mode)
    
    # Docker Spark for hybrid mode (use Spark Connect)
    if spark_mode == "docker":
        return _create_docker_spark_session(cfg, app_name, ref)
    
    # Cluster Spark (requires external Spark cluster - Docker mode)
    return _create_cluster_spark_session(cfg, app_name, ref)


def _create_local_spark_session(cfg: dict, app_name: str, ref: str, execution_mode: str) -> SparkSession:
    """Create local Spark session with local or remote catalog."""
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    
    catalog_type = cfg["catalog"].get("type", "remote")
    
    if catalog_type == "local":
        # Use local Iceberg catalog with file-based storage
        return (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
            .config("spark.sql.catalog.local.warehouse", cfg["catalog"]["warehouse"])
            .config("spark.sql.defaultCatalog", "local")
            .getOrCreate()
        )
    else:
        # Use remote Nessie catalog (hybrid mode)
        return (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            .config("spark.sql.catalog.nessie.uri", cfg["catalog"]["uri"])
            .config("spark.sql.catalog.nessie.ref", ref)
            .config("spark.sql.catalog.nessie.warehouse", cfg["catalog"]["warehouse"])
            .config("spark.sql.catalog.nessie.authentication.type", "NONE")
            .config("spark.sql.defaultCatalog", "nessie")
            .config("spark.hadoop.fs.s3a.endpoint", cfg["storage"]["endpoint"])
            .config("spark.hadoop.fs.s3a.access.key", cfg["storage"].get("access_key", os.getenv("AWS_ACCESS_KEY_ID", "risk_analytics_admin")))
            .config("spark.hadoop.fs.s3a.secret.key", cfg["storage"].get("secret_key", os.getenv("AWS_SECRET_ACCESS_KEY", "risk_analytics_local_development_secret")))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )


def _create_docker_spark_session(cfg: dict, app_name: str, ref: str) -> SparkSession:
    """Create Spark session that connects to Docker Spark services."""
    # Use Spark Connect to connect to Docker Spark
    remote = os.getenv("SPARK_REMOTE", "sc://localhost:15002")
    return SparkSession.builder.appName(app_name).remote(remote).getOrCreate()


def _create_cluster_spark_session(cfg: dict, app_name: str, ref: str) -> SparkSession:
    """Create cluster Spark session for Docker mode."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", cfg["catalog"]["nessie_uri"])
        .config("spark.sql.catalog.nessie.ref", ref)
        .config("spark.sql.catalog.nessie.warehouse", cfg["catalog"]["warehouse"])
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.hadoop.fs.s3a.endpoint", cfg["storage"]["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "risk_analytics_admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "risk_analytics_local_development_secret"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

