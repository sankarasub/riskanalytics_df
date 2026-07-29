"""Spark session construction for the Nessie-backed Iceberg catalog."""
from __future__ import annotations

import os
from pyspark.sql import SparkSession
from risk_analytics.config import load_config

def create_spark_session(app_name: str, ref: str = "main") -> SparkSession:
    """Create the project Spark entry point for remote notebooks or batch jobs.

    ``SPARK_REMOTE`` selects Spark Connect for interactive clients. Batch jobs
    receive the full Iceberg, Nessie, and S3A configuration so every writer uses
    the requested catalog reference and the same warehouse contract.
    """
    cfg = load_config()
    # Use Spark Connect only when explicitly requested by environment.
    # Batch spark-submit jobs unset SPARK_REMOTE and should build a classic
    # SparkSession with master-driven configuration.
    remote = os.getenv("SPARK_REMOTE")

    if remote:
        # Spark Connect already owns server-side catalog configuration.
        return SparkSession.builder.appName(app_name).remote(remote).getOrCreate()

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

