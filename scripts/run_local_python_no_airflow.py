"""Run the full Risk Analytics flow from local Python without Airflow."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from risk_analytics.spark import create_spark_session


ENTITIES = ["customer", "asset", "collateral", "deals"]
SOURCE_B_PATHS = {
    "customer_sourceb_path": "/opt/risk_analytics/data/sourceb/customer/*.csv",
    "asset_sourceb_path": "/opt/risk_analytics/data/sourceb/asset/*.json",
    "product_sourceb_path": "/opt/risk_analytics/data/sourceb/product/*.json",
    "trans_sourceb_path": "/opt/risk_analytics/data/sourceb/trans/*.csv",
    "collateral_sourceb_path": "/opt/risk_analytics/data/sourceb/collateral/*.json",
}


def run_command(command: list[str], env: dict[str, str], cwd: Path = REPO_ROOT) -> None:
    print("$", " ".join(command))
    result = subprocess.run(command, cwd=str(cwd), env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {' '.join(command)}")


def docker_startup(mode: str, env: dict[str, str]) -> None:
    if mode == "fresh":
        run_command(["docker", "compose", "down", "-v"], env)
        run_command(["docker", "compose", "up", "--build", "-d"], env)
    elif mode == "reuse":
        run_command(["docker", "compose", "up", "-d", "--no-build"], env)
    elif mode == "none":
        print("Skipping Docker startup (--docker-mode none).")
    else:  # pragma: no cover
        raise ValueError(f"Unsupported docker mode: {mode}")

    if mode != "none":
        run_command(["docker", "compose", "ps", "--all"], env)


def run_bootstrap(python_executable: str, as_of_date: str, env: dict[str, str]) -> None:
    run_command(
        [
            python_executable,
            str(REPO_ROOT / "jobs" / "bootstrap.py"),
            "--action",
            "all",
            "--as-of-date",
            as_of_date,
        ],
        env,
    )


def run_source_to_ods(python_executable: str, as_of_date: str, source: str, env: dict[str, str]) -> None:
    step_runner = str(REPO_ROOT / "jobs" / "run_source_to_ods_step.py")
    source_b_params = []
    if source == "sourceb":
        for key, value in SOURCE_B_PATHS.items():
            source_b_params.extend(["--param", f"{key}={value}"])

    for entity in ENTITIES:
        for layer in ("stage", "ods"):
            run_command(
                [
                    python_executable,
                    step_runner,
                    "--layer",
                    layer,
                    "--entity",
                    entity,
                    "--source",
                    source,
                    "--as-of-date",
                    as_of_date,
                    *source_b_params,
                ],
                env,
            )


def print_top5_ods() -> dict[str, int]:
    spark = create_spark_session("local-ods-check")
    counts: dict[str, int] = {}
    try:
        tables = ["customer", "asset", "collateral", "deals"]
        for table in tables:
            counts[table] = spark.sql(f"SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.{table}").collect()[0]["c"]
            print(f"\n=== {table} (top 5) ===")
            spark.sql(f"SELECT * FROM nessie.risk_analytics_ods.{table} LIMIT 5").show(truncate=False)
    finally:
        spark.stop()
    return counts


def run_risk_pipeline(python_executable: str, as_of_date: str, env: dict[str, str]) -> str:
    run_id = f"local-manual-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    run_command(
        [
            python_executable,
            str(REPO_ROOT / "jobs" / "run_risk_pipeline.py"),
            "--as-of-date",
            as_of_date,
            "--run-id",
            run_id,
            "--data-model",
            "source-to-ods",
        ],
        env,
    )
    return run_id


def print_top5_risk_output(as_of_date: str) -> int:
    spark = create_spark_session("local-risk-output-check")
    try:
        print(f"\n=== risk_metrics count for {as_of_date} ===")
        risk_count = spark.sql(
            f"""
            SELECT COUNT(*) AS row_count
            FROM nessie.risk_analytics_ods.risk_metrics
            WHERE as_of_date = DATE '{as_of_date}'
            """
        ).collect()[0]["row_count"]
        print(f"row_count = {risk_count}")

        print(f"\n=== risk_metrics top 5 for {as_of_date} ===")
        spark.sql(
            f"""
            SELECT *
            FROM nessie.risk_analytics_ods.risk_metrics
            WHERE as_of_date = DATE '{as_of_date}'
            ORDER BY calculation_timestamp DESC
            LIMIT 5
            """
        ).show(truncate=False)
    finally:
        spark.stop()
    return int(risk_count)


def write_run_report(
    output_dir: Path,
    run_timestamp: str,
    run_meta: dict[str, Any],
    ods_counts: dict[str, int],
    risk_row_count: int,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"local_no_airflow_run_{run_timestamp}.md"

    lines = [
        "# Local Python No-Airflow Run Report",
        "",
        "## Run Metadata",
        "",
        f"- run_timestamp: {run_timestamp}",
        f"- started_at_utc: {run_meta['started_at_utc']}",
        f"- finished_at_utc: {run_meta['finished_at_utc']}",
        f"- duration_seconds: {run_meta['duration_seconds']}",
        f"- as_of_date: {run_meta['as_of_date']}",
        f"- docker_mode: {run_meta['docker_mode']}",
        f"- source_mode: {run_meta['source_mode']}",
        f"- spark_remote: {run_meta['spark_remote']}",
        f"- nessie_uri: {run_meta['nessie_uri']}",
        f"- python_executable: {run_meta['python_executable']}",
        f"- run_id: {run_meta['run_id']}",
        "",
        "## ODS Table Counts",
        "",
        "| table | row_count |",
        "| --- | ---: |",
    ]

    for table_name, row_count in ods_counts.items():
        lines.append(f"| {table_name} | {row_count} |")

    lines.extend(
        [
            "",
            "## Risk Output",
            "",
            f"- risk_metrics row_count for {run_meta['as_of_date']}: {risk_row_count}",
            "",
            "## Notes",
            "",
            "- Top-5 result previews were printed to the terminal during this run.",
            "- This report captures run parameters and count-level validation outputs.",
        ]
    )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local Python workflow without Airflow")
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--docker-mode", choices=["fresh", "reuse", "none"], default="reuse")
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument(
        "--source-mode",
        choices=["sourcea", "sourceb", "both"],
        default="sourcea",
        help="Which source input to process through stage+ODS before final risk calculation.",
    )
    parser.add_argument(
        "--include-sourceb",
        action="store_true",
        help="Deprecated alias for --source-mode both.",
    )
    parser.add_argument(
        "--spark-remote",
        default="sc://localhost:15002",
        help="Spark Connect endpoint for local Python jobs.",
    )
    parser.add_argument(
        "--nessie-uri",
        default="http://localhost:19120/api/v2",
        help="Nessie API endpoint for local Python jobs.",
    )
    parser.add_argument(
        "--run-info-dir",
        default="logs/run-info",
        help="Directory for generated markdown run reports.",
    )
    parser.add_argument(
        "--skip-run-info",
        action="store_true",
        help="Do not write a markdown run report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started_at = datetime.utcnow()
    run_timestamp = started_at.strftime("%Y%m%d_%H%M%S")

    env = os.environ.copy()
    env["SPARK_REMOTE"] = args.spark_remote
    env["NESSIE_URI"] = args.nessie_uri
    os.environ["SPARK_REMOTE"] = args.spark_remote
    os.environ["NESSIE_URI"] = args.nessie_uri

    print("Running no-Airflow local workflow with settings:")
    print(f"  as_of_date     : {args.as_of_date}")
    print(f"  docker_mode    : {args.docker_mode}")
    source_mode = args.source_mode
    if args.include_sourceb and source_mode == "sourcea":
        source_mode = "both"
    print(f"  source_mode    : {source_mode}")
    print(f"  spark_remote   : {args.spark_remote}")
    print(f"  nessie_uri     : {args.nessie_uri}")

    docker_startup(args.docker_mode, env)
    run_bootstrap(args.python_executable, args.as_of_date, env)
    if source_mode in {"sourcea", "both"}:
        run_source_to_ods(args.python_executable, args.as_of_date, "sourcea", env)
    if source_mode in {"sourceb", "both"}:
        run_source_to_ods(args.python_executable, args.as_of_date, "sourceb", env)
    ods_counts = print_top5_ods()
    run_id = run_risk_pipeline(args.python_executable, args.as_of_date, env)
    risk_row_count = print_top5_risk_output(args.as_of_date)

    finished_at = datetime.utcnow()
    duration_seconds = int((finished_at - started_at).total_seconds())

    report_path: Path | None = None
    if not args.skip_run_info:
        run_meta = {
            "started_at_utc": started_at.isoformat(timespec="seconds") + "Z",
            "finished_at_utc": finished_at.isoformat(timespec="seconds") + "Z",
            "duration_seconds": duration_seconds,
            "as_of_date": args.as_of_date,
            "docker_mode": args.docker_mode,
            "source_mode": source_mode,
            "spark_remote": args.spark_remote,
            "nessie_uri": args.nessie_uri,
            "python_executable": args.python_executable,
            "run_id": run_id,
        }
        report_path = write_run_report(
            output_dir=REPO_ROOT / args.run_info_dir,
            run_timestamp=run_timestamp,
            run_meta=run_meta,
                ods_counts=ods_counts,
            risk_row_count=risk_row_count,
        )

    print("\nWorkflow completed successfully.")
    print(f"Run ID: {run_id}")
    print(f"risk_metrics row_count ({args.as_of_date}): {risk_row_count}")
    if report_path is not None:
        print(f"Run report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())