"""Initialize the source-to-ODS schema and load small deterministic development data."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Set execution mode to local by default
if not os.environ.get("EXECUTION_MODE"):
    os.environ["EXECUTION_MODE"] = "local"

# Set JAVA_HOME for PySpark
if not os.environ.get("JAVA_HOME"):
    java_home = "C:\\Program Files\\Java\\jdk-24"
    if os.path.exists(java_home):
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = f"{java_home}\\bin;" + os.environ.get("PATH", "")

from pyspark.sql.types import BooleanType, DateType, DecimalType, MapType, StringType, StructField, StructType

from risk_analytics.config import legacy_table_name, load_config
from risk_analytics.logging_config import PipelineLogger, setup_logging
from risk_analytics.spark import create_spark_session

DEFAULT_AS_OF_DATE = date(2026, 7, 18)
SOURCEA_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "sourcea"
SOURCE_TABLES = ("customer", "asset", "collateral", "deals")
SOURCE_TO_ODS_CREATE_ORDER = (
    "namespace_stage",
    "namespace_ods",
    "deals",
    "customer_stage_sourcea",
    "customer_stage_sourceb",
    "asset_stage_sourcea",
    "asset_stage_sourceb",
    "collateral_stage_sourcea",
    "collateral_stage_sourceb",
    "deals_stage_sourcea",
    "deals_stage_sourceb",
    "ods_customer",
    "ods_asset",
    "ods_collateral",
    "ods_deals",
    "ods_risk_metrics",
)

SCHEMAS = {
    "customer": StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("legal_entity_id", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("active_flag", BooleanType(), True),
        StructField("as_of_date", DateType(), True),
    ]),
    "asset": StructType([
        StructField("asset_id", StringType(), False),
        StructField("isin", StringType(), True),
        StructField("asset_class", StringType(), True),
        StructField("issuer", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("market_value", DecimalType(20, 2), True),
        StructField("valuation_date", DateType(), True),
    ]),
    "collateral": StructType([
        StructField("collateral_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("asset_id", StringType(), True),
        StructField("agreement_id", StringType(), True),
        StructField("collateral_type", StringType(), True),
        StructField("quantity", DecimalType(20, 6), True),
        StructField("market_value", DecimalType(20, 2), True),
        StructField("currency", StringType(), True),
        StructField("valuation_date", DateType(), True),
    ]),
    "deals": StructType([
        StructField("deal_id", StringType(), False),
        StructField("trade_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("asset_id", StringType(), True),
        StructField("collateral_id", StringType(), True),
        StructField("netting_set_id", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("trade_date", DateType(), True),
        StructField("maturity_date", DateType(), True),
        StructField("currency", StringType(), True),
        StructField("notional", DecimalType(20, 2), True),
        StructField("mark_to_market", DecimalType(20, 2), True),
        StructField("status", StringType(), True),
        StructField("as_of_date", DateType(), True),
        StructField("volatility", DecimalType(10, 6), True),
        StructField("fixed_rate", DecimalType(12, 8), True),
        StructField("strike", DecimalType(20, 6), True),
        StructField("option_type", StringType(), True),
    ]),
}

DATE_COLUMNS = {
    "customer": {"as_of_date"},
    "asset": {"valuation_date"},
    "collateral": {"valuation_date"},
    "deals": {"trade_date", "maturity_date", "as_of_date"},
}

DECIMAL_COLUMNS = {
    "asset": {"market_value"},
    "collateral": {"quantity", "market_value"},
    "deals": {"notional", "mark_to_market", "volatility", "fixed_rate", "strike"},
}

BOOL_COLUMNS = {
    "customer": {"active_flag"},
}


def seed_data(as_of_date: date) -> dict[str, dict[str, object]]:
    """Prepare typed, deterministic seed data for the source-layer tables."""
    definitions = {
        table_name: {
            "rows": _load_seed_rows(table_name, as_of_date),
            "schema": SCHEMAS[table_name],
        }
        for table_name in SOURCE_TABLES if table_name != "deals"
    }
    definitions["deals"] = {
        "rows": _build_deals_rows(as_of_date),
        "schema": SCHEMAS["deals"],
    }
    return definitions


def _load_seed_rows(table_name: str, as_of_date: date) -> list[dict[str, object]]:
    file_path = SOURCEA_DATA_DIR / f"{table_name}.json"
    with file_path.open(encoding="utf-8") as source:
        raw_rows = json.load(source)
    if not isinstance(raw_rows, list):
        raise ValueError(f"Seed file must contain an array of rows: {file_path}")
    return [_normalize_row(table_name, row, as_of_date) for row in raw_rows]


def _build_deals_rows(as_of_date: date) -> list[dict[str, object]]:
    """Build deals rows by loading from deals.json file."""
    try:
        file_path = SOURCEA_DATA_DIR / "deals.json"
        with file_path.open(encoding="utf-8") as source:
            raw_rows = json.load(source)
        if not isinstance(raw_rows, list):
            raise ValueError(f"Seed file must contain an array of rows: {file_path}")
        return [_normalize_row("deals", row, as_of_date) for row in raw_rows]
    except FileNotFoundError:
        # If deals.json doesn't exist, return empty list for now
        # In production, this file should exist
        return []


def _normalize_row(table_name: str, row: dict[str, object], as_of_date: date) -> dict[str, object]:
    """Convert JSON-friendly source values into the declared Spark schema types.

    Keeping conversion at the ingestion boundary prevents implicit Spark casts
    from hiding data-quality issues and ensures the same demo data produces the
    same table types on every machine.
    """
    if not isinstance(row, dict):
        raise ValueError(f"Seed row for '{table_name}' must be an object.")

    output: dict[str, object] = {}
    for key, value in row.items():
        # Sample files use a placeholder so one seed set supports any run date.
        if value == "{{as_of_date}}":
            value = as_of_date.isoformat()

        if key in DATE_COLUMNS.get(table_name, set()) and isinstance(value, str):
            output[key] = date.fromisoformat(value)
            continue

        if key in DECIMAL_COLUMNS.get(table_name, set()) and value is not None:
            output[key] = Decimal(str(value))
            continue

        if key in BOOL_COLUMNS.get(table_name, set()) and isinstance(value, str):
            output[key] = value.strip().lower() in {"true", "1", "y", "yes"}
            continue

        output[key] = value
    return output


def create_namespace(spark) -> None:
    spark.sql(TABLE_DDL["namespace"])


def create_table(spark, table_name: str) -> None:
    spark.sql(TABLE_DDL[table_name])


def create_all_tables(spark) -> None:
    """Create the complete source-to-metrics table contract in dependency order."""
    config = load_config()
    execution_mode = config.get("execution_mode", "docker")
    
    create_namespace(spark)
    
    # Skip source tables in local/hybrid modes
    if execution_mode == "docker":
        for table_name in SOURCE_TABLES:
            create_table(spark, table_name)
    
    # Always create stage/ODS tables
    for table_name in SOURCE_TO_ODS_CREATE_ORDER:
        create_table(spark, table_name)


def create_source_to_ods_tables(spark) -> None:
    """Create the new stage/ODS contracts for customer, asset, collateral, and deals."""
    create_namespace(spark)
    for table_name in SOURCE_TO_ODS_CREATE_ORDER:
        create_table(spark, table_name)


def seed_table(spark, table_name: str, as_of_date: date) -> None:
    """Append seed rows only when the requested source table is empty.

    The empty-table check makes bootstrap safe to repeat: it prepares a new
    environment without duplicating demo records in an already initialized one.
    """
    supported_tables = SOURCE_TABLES
    if table_name not in supported_tables:
        raise ValueError(f"No seed data exists for table '{table_name}'.")

    definition = seed_data(as_of_date)[table_name]

    config = load_config()
    table_ref = legacy_table_name(config, table_name)
    if spark.table(table_ref).limit(1).count() == 0:
        frame = spark.createDataFrame(definition["rows"], schema=definition["schema"])
        frame.writeTo(table_ref).append()


def seed_all_tables(spark, as_of_date: date) -> None:
    """Seed source tables only in Docker mode where they are used."""
    config = load_config()
    execution_mode = config.get("execution_mode", "docker")
    
    if execution_mode == "docker":
        for table_name in SOURCE_TABLES:
            seed_table(spark, table_name, as_of_date)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action",
        choices=["all", "create-namespace", "create-table", "create-all", "create-all-source-to-ods", "seed-table", "seed-all"],
        default="all",
    )
    parser.add_argument("--table")
    parser.add_argument("--as-of-date", default=DEFAULT_AS_OF_DATE.isoformat())
    return parser.parse_args()


def main(action: str, table_name: str | None, as_of_date: date) -> None:
    """Dispatch a bootstrap action and guarantee Spark cleanup for every path."""
    # Setup logging
    setup_logging()
    logger = PipelineLogger("bootstrap")
    
    config = load_config()
    execution_mode = config.get("execution_mode", "docker")
    
    params = {
        "action": action,
        "table_name": table_name,
        "as_of_date": as_of_date.isoformat(),
        "execution_mode": execution_mode
    }
    
    logger.log_pipeline_start(params)
    
    spark = create_spark_session("risk-analytics-bootstrap")
    logger.log_spark_operation("session_created", {"app_name": "risk-analytics-bootstrap"})
    
    try:
        if action == "all":
            logger.log_step_start("create_all_tables")
            create_all_tables(spark)
            logger.log_step_complete("create_all_tables")
            
            if execution_mode == "docker":
                logger.log_step_start("seed_all_tables")
                seed_all_tables(spark, as_of_date)
                logger.log_step_complete("seed_all_tables")
            else:
                logger.log_step_start("seed_all_tables", {"skipped": True, "reason": f"{execution_mode} mode - only for Docker mode"})
                logger.log_step_complete("seed_all_tables", 0, 0)
                
        elif action == "create-namespace":
            logger.log_step_start("create_namespace")
            create_namespace(spark)
            logger.log_step_complete("create_namespace")
            
        elif action == "create-table":
            if not table_name:
                raise ValueError("--table is required for create-table.")
            logger.log_step_start(f"create_table_{table_name}")
            create_table(spark, table_name)
            logger.log_step_complete(f"create_table_{table_name}")
            
        elif action == "create-all":
            logger.log_step_start("create_all_tables")
            create_all_tables(spark)
            logger.log_step_complete("create_all_tables")
            
        elif action == "create-all-source-to-ods":
            logger.log_step_start("create_source_to_ods_tables")
            create_source_to_ods_tables(spark)
            logger.log_step_complete("create_source_to_ods_tables")
            
        elif action == "seed-table":
            if not table_name:
                raise ValueError("--table is required for seed-table.")
            if execution_mode != "docker":
                logger.log_step_start(f"seed_table_{table_name}", {"skipped": True, "reason": f"{execution_mode} mode"})
                logger.log_step_complete(f"seed_table_{table_name}", 0, 0)
                print(f"Skipping seed-table in {execution_mode} mode (only for Docker mode)")
                return
            logger.log_step_start(f"seed_table_{table_name}")
            start_time = time.time()
            seed_table(spark, table_name, as_of_date)
            duration = (time.time() - start_time) * 1000
            logger.log_step_complete(f"seed_table_{table_name}", duration_ms=duration)
            
        elif action == "seed-all":
            if execution_mode != "docker":
                logger.log_step_start("seed_all_tables", {"skipped": True, "reason": f"{execution_mode} mode"})
                logger.log_step_complete("seed_all_tables", 0, 0)
                print(f"Skipping seed-all in {execution_mode} mode (only for Docker mode)")
                return
            logger.log_step_start("seed_all_tables")
            start_time = time.time()
            seed_all_tables(spark, as_of_date)
            duration = (time.time() - start_time) * 1000
            logger.log_step_complete("seed_all_tables", duration_ms=duration)
            
        else:
            raise ValueError(f"Unsupported action '{action}'.")
        
        logger.log_pipeline_complete(success=True)
        
    except Exception as e:
        logger.log_step_error(action, e)
        logger.log_pipeline_complete(success=False)
        raise
    finally:
        spark.stop()
        logger.log_spark_operation("session_stopped")


if __name__ == "__main__":
    args = parse_args()
    main(args.action, args.table, date.fromisoformat(args.as_of_date))

