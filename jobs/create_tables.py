"""Create Iceberg table contracts for legacy and source-to-ODS migration flows."""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from risk_analytics.config import load_config
from risk_analytics.spark import create_spark_session


def _table_ref(catalog_name: str, namespace: str, table_name: str) -> str:
    return f"{catalog_name}.{namespace}.{table_name}"


def build_table_ddl(config: dict) -> dict[str, str]:
    catalog = config.get("catalog", {})
    catalog_name = catalog.get("name", "nessie")
    legacy_namespace = catalog.get("namespace", "risk_analytics")
    stage_namespace = catalog.get("stage_namespace", "risk_analytics_stage")
    ods_namespace = catalog.get("ods_namespace", "risk_analytics_ods")
    execution_mode = config.get("execution_mode", "docker")

    ddl: dict[str, str] = {
        "namespace": f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{legacy_namespace}",
        "namespace_stage": f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{stage_namespace}",
        "namespace_ods": f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{ods_namespace}",
    }

    # Only create source tables in Docker mode
    if execution_mode == "docker":
        ddl["customer"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, legacy_namespace, 'customer')} (
            customer_id STRING, customer_name STRING, legal_entity_id STRING, rating STRING,
            country_code STRING, entity_type STRING, active_flag BOOLEAN, as_of_date DATE
        ) USING iceberg PARTITIONED BY (days(as_of_date))"""
        ddl["asset"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, legacy_namespace, 'asset')} (
            asset_id STRING, isin STRING, asset_class STRING, issuer STRING, currency STRING,
            market_value DECIMAL(20,2), valuation_date DATE
        ) USING iceberg PARTITIONED BY (asset_class, days(valuation_date))"""
        ddl["collateral"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, legacy_namespace, 'collateral')} (
            collateral_id STRING, customer_id STRING, asset_id STRING, agreement_id STRING,
            collateral_type STRING, quantity DECIMAL(20,6), market_value DECIMAL(20,2), currency STRING,
            valuation_date DATE
        ) USING iceberg PARTITIONED BY (days(valuation_date))"""
        ddl["deals"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, legacy_namespace, 'deals')} (
            deal_id STRING, trade_id STRING, customer_id STRING, asset_id STRING, collateral_id STRING,
            netting_set_id STRING, product_type STRING, trade_date DATE, maturity_date DATE,
            currency STRING, notional DECIMAL(20,2), mark_to_market DECIMAL(20,2),
            status STRING, as_of_date DATE, volatility DECIMAL(10,6),
            fixed_rate DECIMAL(12,8), strike DECIMAL(20,6), option_type STRING
        ) USING iceberg PARTITIONED BY (product_type, days(as_of_date))"""

    # Risk metrics table placement depends on execution mode
    if execution_mode == "docker":
        risk_metrics_namespace = legacy_namespace
    else:
        risk_metrics_namespace = ods_namespace
    
    ddl["risk_metrics"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, risk_metrics_namespace, 'risk_metrics')} (
        risk_run_id STRING, as_of_date DATE, customer_id STRING, netting_set_id STRING,
        gross_exposure DECIMAL(20,2), netting_exposure DECIMAL(20,2),
        collateral_value_after_haircut DECIMAL(20,2), pfe DECIMAL(20,2), var DECIMAL(20,2),
        calculation_timestamp TIMESTAMP, source_branch STRING
    ) USING iceberg PARTITIONED BY (days(as_of_date))"""

    # Stage namespace tables (always created)

    # New stage namespace tables.
    ddl["customer_stage_sourcea"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'customer_stage_sourcea')} (
        customer_id STRING, customer_name STRING, legal_entity_id STRING, rating STRING,
        country_code STRING, entity_type STRING, active_flag BOOLEAN, as_of_date DATE,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(as_of_date))"""
    ddl["customer_stage_sourceb"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'customer_stage_sourceb')} (
        customer_id STRING, customer_name STRING, legal_entity_id STRING, rating STRING,
        country_code STRING, entity_type STRING, active_flag BOOLEAN, as_of_date DATE,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(as_of_date))"""

    ddl["asset_stage_sourcea"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'asset_stage_sourcea')} (
        asset_id STRING, isin STRING, asset_class STRING, issuer STRING, currency STRING,
        market_value DECIMAL(20,2), valuation_date DATE, customer_id STRING,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (asset_class, days(valuation_date))"""
    ddl["asset_stage_sourceb"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'asset_stage_sourceb')} (
        asset_id STRING, isin STRING, asset_class STRING, issuer STRING, currency STRING,
        market_value DECIMAL(20,2), valuation_date DATE, customer_id STRING,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (asset_class, days(valuation_date))"""

    ddl["collateral_stage_sourcea"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'collateral_stage_sourcea')} (
        collateral_id STRING, customer_id STRING, asset_id STRING, agreement_id STRING,
        collateral_type STRING, quantity DECIMAL(20,6), market_value DECIMAL(20,2), currency STRING,
        valuation_date DATE, source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(valuation_date))"""
    ddl["collateral_stage_sourceb"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'collateral_stage_sourceb')} (
        collateral_id STRING, customer_id STRING, asset_id STRING, agreement_id STRING,
        collateral_type STRING, quantity DECIMAL(20,6), market_value DECIMAL(20,2), currency STRING,
        valuation_date DATE, source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(valuation_date))"""

    ddl["deals_stage_sourcea"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'deals_stage_sourcea')} (
        deal_id STRING, trade_id STRING, customer_id STRING, asset_id STRING, collateral_id STRING,
        netting_set_id STRING, product_type STRING, trade_date DATE, maturity_date DATE,
        currency STRING, notional DECIMAL(20,2), mark_to_market DECIMAL(20,2),
        status STRING, as_of_date DATE, volatility DECIMAL(10,6),
        fixed_rate DECIMAL(12,8), strike DECIMAL(20,6), option_type STRING,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (product_type, days(as_of_date))"""
    ddl["deals_stage_sourceb"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, stage_namespace, 'deals_stage_sourceb')} (
        deal_id STRING, trade_id STRING, customer_id STRING, asset_id STRING, collateral_id STRING,
        netting_set_id STRING, product_type STRING, trade_date DATE, maturity_date DATE,
        currency STRING, notional DECIMAL(20,2), mark_to_market DECIMAL(20,2),
        status STRING, as_of_date DATE, volatility DECIMAL(10,6),
        fixed_rate DECIMAL(12,8), strike DECIMAL(20,6), option_type STRING,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (product_type, days(as_of_date))"""

    # New ODS standardized tables.
    ddl["ods_customer"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, ods_namespace, 'customer')} (
        customer_id STRING, customer_name STRING, legal_entity_id STRING, rating STRING,
        country_code STRING, entity_type STRING, active_flag BOOLEAN, as_of_date DATE,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(as_of_date))"""
    ddl["ods_asset"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, ods_namespace, 'asset')} (
        asset_id STRING, isin STRING, asset_class STRING, issuer STRING, currency STRING,
        market_value DECIMAL(20,2), valuation_date DATE, customer_id STRING,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (asset_class, days(valuation_date))"""
    ddl["ods_collateral"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, ods_namespace, 'collateral')} (
        collateral_id STRING, customer_id STRING, asset_id STRING, agreement_id STRING,
        collateral_type STRING, quantity DECIMAL(20,6), market_value DECIMAL(20,2), currency STRING,
        valuation_date DATE, source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(valuation_date))"""
    ddl["ods_deals"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, ods_namespace, 'deals')} (
        deal_id STRING, trade_id STRING, customer_id STRING, asset_id STRING, collateral_id STRING,
        netting_set_id STRING, product_type STRING, trade_date DATE, maturity_date DATE,
        currency STRING, notional DECIMAL(20,2), mark_to_market DECIMAL(20,2),
        status STRING, as_of_date DATE, volatility DECIMAL(10,6),
        fixed_rate DECIMAL(12,8), strike DECIMAL(20,6), option_type STRING,
        source_system STRING, ingest_timestamp TIMESTAMP
    ) USING iceberg PARTITIONED BY (product_type, days(as_of_date))"""
    ddl["ods_risk_metrics"] = f"""CREATE TABLE IF NOT EXISTS {_table_ref(catalog_name, ods_namespace, 'risk_metrics')} (
        risk_run_id STRING, as_of_date DATE, customer_id STRING, netting_set_id STRING,
        gross_exposure DECIMAL(20,2), netting_exposure DECIMAL(20,2),
        collateral_value_after_haircut DECIMAL(20,2), pfe DECIMAL(20,2), var DECIMAL(20,2),
        calculation_timestamp TIMESTAMP, source_branch STRING
    ) USING iceberg PARTITIONED BY (days(as_of_date))"""
    return ddl


TABLE_DDL = build_table_ddl(load_config())
DDL = list(TABLE_DDL.values())


if __name__ == "__main__":
    spark = create_spark_session("risk-analytics-create-tables")
    for statement in DDL:
        spark.sql(statement)
    spark.stop()

