"""Initialize the canonical schema and load small deterministic development data."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

from pyspark.sql.types import BooleanType, DateType, DecimalType, MapType, StringType, StructField, StructType

from jobs.create_tables import TABLE_DDL
from risk_analytics.config import legacy_table_name, load_config
from risk_analytics.spark import create_spark_session

DEFAULT_AS_OF_DATE = date(2026, 7, 18)
SOURCEA_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "sourcea"
SOURCE_TABLES = ("customer", "asset", "collateral", "trades", "trade_product", "deals")
STAGING_TABLES = (
    "customer_sourcea_stg",
    "customer_sourceb_stg",
    "asset_sourcea_stg",
    "asset_sourceb_stg",
    "trade_product_sourcea_stg",
    "trade_product_sourceb_stg",
    "trades_sourcea_stg",
    "trades_sourceb_stg",
    "collateral_sourcea_stg",
    "collateral_sourceb_stg",
)
CANONICAL_TABLES = (
    "customer_canonical",
    "asset_canonical",
    "collateral_canonical",
    "trades_canonical",
    "trade_product_canonical",
)
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
    "trades": StructType([
        StructField("trade_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("netting_set_id", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("trade_date", DateType(), True),
        StructField("maturity_date", DateType(), True),
        StructField("currency", StringType(), True),
        StructField("notional", DecimalType(20, 2), True),
        StructField("mark_to_market", DecimalType(20, 2), True),
        StructField("status", StringType(), True),
        StructField("as_of_date", DateType(), True),
    ]),
    "trade_product": StructType([
        StructField("trade_id", StringType(), False),
        StructField("product_type", StringType(), True),
        StructField("underlying_id", StringType(), True),
        StructField("pay_leg_currency", StringType(), True),
        StructField("receive_leg_currency", StringType(), True),
        StructField("fixed_rate", DecimalType(12, 8), True),
        StructField("floating_index", StringType(), True),
        StructField("strike", DecimalType(20, 6), True),
        StructField("option_type", StringType(), True),
        StructField("volatility", DecimalType(10, 6), True),
        StructField("barrier_level", DecimalType(20, 6), True),
        StructField("product_attributes", MapType(StringType(), StringType()), True),
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
    "trades": {"trade_date", "maturity_date", "as_of_date"},
    "trade_product": set(),
    "deals": {"trade_date", "maturity_date", "as_of_date"},
}

DECIMAL_COLUMNS = {
    "asset": {"market_value"},
    "collateral": {"quantity", "market_value"},
    "trades": {"notional", "mark_to_market"},
    "trade_product": {"fixed_rate", "strike", "volatility", "barrier_level"},
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
    trades = _load_seed_rows("trades", as_of_date)
    products = _load_seed_rows("trade_product", as_of_date)
    collateral = _load_seed_rows("collateral", as_of_date)

    product_by_trade = {row["trade_id"]: row for row in products}
    collateral_by_customer: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in collateral:
        collateral_by_customer[str(row.get("customer_id"))].append(row)

    output: list[dict[str, object]] = []
    for row in trades:
        trade_id = str(row["trade_id"])
        customer_id = str(row.get("customer_id"))
        product = product_by_trade.get(trade_id, {})
        customer_collateral = collateral_by_customer.get(customer_id, [])
        best_collateral = customer_collateral[0] if customer_collateral else {}

        output.append({
            "deal_id": trade_id,
            "trade_id": trade_id,
            "customer_id": row.get("customer_id"),
            "asset_id": best_collateral.get("asset_id"),
            "collateral_id": best_collateral.get("collateral_id"),
            "netting_set_id": row.get("netting_set_id"),
            "product_type": row.get("product_type"),
            "trade_date": row.get("trade_date"),
            "maturity_date": row.get("maturity_date"),
            "currency": row.get("currency"),
            "notional": row.get("notional"),
            "mark_to_market": row.get("mark_to_market"),
            "status": row.get("status"),
            "as_of_date": row.get("as_of_date"),
            "volatility": product.get("volatility"),
            "fixed_rate": product.get("fixed_rate"),
            "strike": product.get("strike"),
            "option_type": product.get("option_type"),
        })
    return output


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
    create_namespace(spark)
    for table_name in SOURCE_TABLES + STAGING_TABLES + CANONICAL_TABLES + ("risk_metrics",):
        create_table(spark, table_name)
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
    spark = create_spark_session("risk-analytics-bootstrap")
    try:
        if action == "all":
            create_all_tables(spark)
            seed_all_tables(spark, as_of_date)
        elif action == "create-namespace":
            create_namespace(spark)
        elif action == "create-table":
            if not table_name:
                raise ValueError("--table is required for create-table.")
            create_table(spark, table_name)
        elif action == "create-all":
            create_all_tables(spark)
        elif action == "create-all-source-to-ods":
            create_source_to_ods_tables(spark)
        elif action == "seed-table":
            if not table_name:
                raise ValueError("--table is required for seed-table.")
            seed_table(spark, table_name, as_of_date)
        elif action == "seed-all":
            seed_all_tables(spark, as_of_date)
        else:
            raise ValueError(f"Unsupported action '{action}'.")
    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args.action, args.table, date.fromisoformat(args.as_of_date))

