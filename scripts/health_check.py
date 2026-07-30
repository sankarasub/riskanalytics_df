#!/usr/bin/env python3
"""Local platform health checks for the Risk Analytics lakehouse stack.

Why this script exists
----------------------
Compose reports whether containers are *up*, not whether the services inside
them answer. This script probes every endpoint a developer or DAG depends on, so
a failed pipeline can be attributed to a specific service instead of guessing.
It is the script `scripts/run_risk_analytics_pipeline.ps1` runs as its final
validation step, and it is safe to run at any time.

What it does, in order
----------------------
1. HTTP probes: Nessie REST, SeaweedFS S3 + master, Spark master/worker UIs,
   Airflow, JupyterLab, both Streamlit UIs, the operations API, Dremio, Kafka UI.
2. TCP probes: Spark master RPC, Spark Connect gRPC, Postgres.
3. Optional (`--check-iceberg`): a real Spark Connect query against
   `nessie.risk_analytics_ods` to prove the catalog and `risk_metrics` are
   readable. This only passes after a pipeline run has published metrics.
4. Prints a table and exits non-zero only when a *required* check fails;
   optional checks degrade to WARN so the script stays usable mid-bootstrap.

Related scripts: `scripts/validate_pipeline_status.py` (Airflow/Nessie/Spark
correctness of the DAG inventory and data) and `scripts/validate_dags.py`
(DAG parsing, no platform required).

Usage examples:
  python scripts/health_check.py
  python scripts/health_check.py --host localhost --timeout 5
  python scripts/health_check.py --check-iceberg

From the host against containers, pass the Docker-internal host used by the
UI containers:
  docker compose exec business-ui python /opt/risk_analytics/scripts/health_check.py
      --host host.docker.internal --postgres-host postgres --check-iceberg
"""
from __future__ import annotations

import argparse
import socket
import sys
import time
from dataclasses import dataclass

import requests


@dataclass
class CheckResult:
    name: str
    status: str
    details: str


@dataclass
class HttpCheck:
    name: str
    url: str
    expected_statuses: set[int]
    required: bool = True


@dataclass
class TcpCheck:
    name: str
    host: str
    port: int


def run_http_check(check: HttpCheck, timeout: float) -> CheckResult:
    """Probe an HTTP endpoint and distinguish required failures from optional ones."""
    started = time.time()
    try:
        response = requests.get(check.url, timeout=timeout)
        elapsed_ms = int((time.time() - started) * 1000)
        if response.status_code in check.expected_statuses:
            return CheckResult(
                name=check.name,
                status="PASS",
                details=f"HTTP {response.status_code} in {elapsed_ms} ms",
            )
        return CheckResult(
            name=check.name,
            status="WARN" if not check.required else "FAIL",
            details=(
                f"HTTP {response.status_code} in {elapsed_ms} ms "
                f"(expected one of {sorted(check.expected_statuses)})"
            ),
        )
    except Exception as error:
        status = "WARN" if not check.required else "FAIL"
        details = f"{type(error).__name__}: {error}"
        if not check.required:
            details = f"optional check skipped: {details}"
        return CheckResult(name=check.name, status=status, details=details)


def run_tcp_check(check: TcpCheck, timeout: float) -> CheckResult:
    started = time.time()
    try:
        with socket.create_connection((check.host, check.port), timeout=timeout):
            elapsed_ms = int((time.time() - started) * 1000)
            return CheckResult(name=check.name, status="PASS", details=f"TCP connect in {elapsed_ms} ms")
    except Exception as error:
        return CheckResult(name=check.name, status="FAIL", details=f"{type(error).__name__}: {error}")


def run_iceberg_check(host: str) -> CheckResult:
    """Optionally verify the full Spark Connect-to-Iceberg query path.

    This is deliberately additive to service probes: a developer can run basic
    checks without PySpark, while the deeper check confirms catalog visibility
    and that the published metrics table is actually queryable.
    """
    spark_remote = f"sc://{host}:15002"
    try:
        from pyspark.sql import SparkSession
    except Exception as error:
        return CheckResult(
            name="iceberg-query",
            status="WARN",
            details=f"optional check skipped: pyspark import failed: {error}",
        )

    try:
        spark = SparkSession.builder.remote(spark_remote).appName("risk-analytics-health-check").getOrCreate()
    except Exception as error:
        return CheckResult(
            name="iceberg-query",
            status="WARN",
            details=f"optional check skipped: Spark Connect session failed for {spark_remote}: {error}",
        )

    try:
        started = time.time()
        table_count = spark.sql("SHOW TABLES IN nessie.risk_analytics_ods").count()
        risk_count = spark.sql("SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics").collect()[0]["c"]
        elapsed_ms = int((time.time() - started) * 1000)
        return CheckResult(
            name="iceberg-query",
            status="PASS",
            details=(
                f"queried catalog via {spark_remote}; tables={table_count}, "
                f"risk_metrics_rows={risk_count}, time={elapsed_ms} ms"
            ),
        )
    except Exception as error:
        return CheckResult(name="iceberg-query", status="WARN", details=f"optional check skipped: query failed: {error}")
    finally:
        spark.stop()


def main() -> int:
    """Run the platform probe suite and return a shell-friendly health status."""
    parser = argparse.ArgumentParser(description="Risk Analytics platform health checks")
    parser.add_argument("--host", default="localhost", help="Host to probe (default: localhost)")
    parser.add_argument(
        "--postgres-host",
        default=None,
        help="Host to use for the Postgres TCP check (defaults to --host)",
    )
    parser.add_argument("--timeout", type=float, default=5.0, help="Timeout in seconds per check")
    parser.add_argument(
        "--check-iceberg",
        action="store_true",
        help="Run a Spark Connect query against nessie.risk_analytics_ods tables (requires pyspark)",
    )
    args = parser.parse_args()

    host = args.host
    postgres_host = args.postgres_host or host
    timeout = args.timeout

    http_checks = [
        HttpCheck("nessie-api", f"http://{host}:19120/api/v2/trees/main", {200}),
        HttpCheck("seaweedfs-s3", f"http://{host}:8333", {200, 403, 405}),
        HttpCheck("seaweedfs-master", f"http://{host}:9333", {200}),
        HttpCheck("spark-master-ui", f"http://{host}:8080", {200}),
        HttpCheck("spark-worker-ui", f"http://{host}:8081", {200}),
        HttpCheck("airflow-health", f"http://{host}:8088/health", {200}),
        HttpCheck("jupyterlab", f"http://{host}:8888", {200, 302}),
        HttpCheck("business-ui", f"http://{host}:8501/_stcore/health", {200}),
        HttpCheck("developer-ui", f"http://{host}:8502/_stcore/health", {200}, required=False),
        # The operations API answers 200 even when a component is degraded, so
        # this probe covers reachability only; read the payload for detail.
        HttpCheck("operations-api", f"http://{host}:8000/health?include_spark=false", {200}),
        HttpCheck("dremio-ui", f"http://{host}:9047", {200, 302, 307}),
        HttpCheck("kafka-ui", f"http://{host}:8090", {200, 302}, required=False),
    ]

    tcp_checks = [
        TcpCheck("spark-master-rpc", host, 7077),
        TcpCheck("spark-connect-grpc", host, 15002),
        TcpCheck("postgres", postgres_host, 5432),
    ]

    results: list[CheckResult] = [run_http_check(http_check, timeout) for http_check in http_checks]
    results.extend(run_tcp_check(tcp_check, timeout) for tcp_check in tcp_checks)

    if args.check_iceberg:
        results.append(run_iceberg_check(host))

    print("Risk Analytics Platform Health Check")
    print("=" * 80)
    print(f"Target host: {host}")
    print(f"Timeout/check: {timeout:.1f}s")
    print("-" * 80)

    name_width = max(len(result.name) for result in results) if results else 20
    for result in results:
        print(f"{result.name.ljust(name_width)}  {result.status:<4}  {result.details}")

    failures = [result for result in results if result.status == "FAIL"]
    warnings = [result for result in results if result.status == "WARN"]
    print("-" * 80)
    print(
        f"Total checks: {len(results)} | Passed: {len(results) - len(failures) - len(warnings)} | "
        f"Failed: {len(failures)} | Warnings: {len(warnings)}"
    )

    if failures:
        print("Health check failed.")
        return 1

    if warnings:
        print("Health check completed with warnings.")
        return 0

    print("All required services are healthy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

