"""Spark Structured Streaming job that lands Kafka entity events in Iceberg.

Topics consumed:
- risk.deals.ingest
- risk.customer.ingest
- risk.asset.ingest
- risk.collateral.ingest

Topic produced:
- risk.pipeline.trigger (one message per entity written in a micro-batch, so each
  ``ra_kafka_<entity>_stage`` DAG only wakes for its own entity)
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, StringType, StructField, StructType

from risk_analytics.kafka_events import TRIGGER_TOPIC, build_trigger_payload

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INGEST_TOPICS = [
    "risk.deals.ingest",
    "risk.customer.ingest",
    "risk.asset.ingest",
    "risk.collateral.ingest",
]
CHECKPOINT_PATH = "/tmp/checkpoints/kafka_entity_stream"

TOPIC_TABLE_MAP = {
    "risk.deals.ingest": "nessie.risk_analytics.deals",
    "risk.customer.ingest": "nessie.risk_analytics.customer",
    "risk.asset.ingest": "nessie.risk_analytics.asset",
    "risk.collateral.ingest": "nessie.risk_analytics.collateral",
}

TOPIC_ENTITY_MAP = {
    "risk.deals.ingest": "deals",
    "risk.customer.ingest": "customer",
    "risk.asset.ingest": "asset",
    "risk.collateral.ingest": "collateral",
}

DEALS_SCHEMA = StructType(
    [
        StructField("deal_id", StringType(), nullable=False),
        StructField("trade_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("asset_id", StringType(), nullable=True),
        StructField("collateral_id", StringType(), nullable=True),
        StructField("netting_set_id", StringType(), nullable=True),
        StructField("product_type", StringType(), nullable=True),
        StructField("trade_date", StringType(), nullable=True),
        StructField("maturity_date", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("notional", DecimalType(20, 2), nullable=True),
        StructField("mark_to_market", DecimalType(20, 2), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("as_of_date", StringType(), nullable=True),
        StructField("volatility", DecimalType(10, 6), nullable=True),
        StructField("fixed_rate", DecimalType(12, 8), nullable=True),
        StructField("strike", DecimalType(20, 6), nullable=True),
        StructField("option_type", StringType(), nullable=True),
    ]
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("customer_name", StringType(), nullable=True),
        StructField("legal_entity_id", StringType(), nullable=True),
        StructField("rating", StringType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("entity_type", StringType(), nullable=True),
        StructField("active_flag", BooleanType(), nullable=True),
        StructField("as_of_date", StringType(), nullable=True),
    ]
)

ASSET_SCHEMA = StructType(
    [
        StructField("asset_id", StringType(), nullable=False),
        StructField("isin", StringType(), nullable=True),
        StructField("asset_class", StringType(), nullable=True),
        StructField("issuer", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("market_value", DecimalType(20, 2), nullable=True),
        StructField("valuation_date", StringType(), nullable=True),
    ]
)

COLLATERAL_SCHEMA = StructType(
    [
        StructField("collateral_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("asset_id", StringType(), nullable=True),
        StructField("agreement_id", StringType(), nullable=True),
        StructField("collateral_type", StringType(), nullable=True),
        StructField("quantity", DecimalType(20, 6), nullable=True),
        StructField("market_value", DecimalType(20, 2), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("valuation_date", StringType(), nullable=True),
    ]
)

TOPIC_SCHEMA_MAP = {
    "risk.deals.ingest": DEALS_SCHEMA,
    "risk.customer.ingest": CUSTOMER_SCHEMA,
    "risk.asset.ingest": ASSET_SCHEMA,
    "risk.collateral.ingest": COLLATERAL_SCHEMA,
}


def _normalize_deals(frame: DataFrame) -> DataFrame:
    return frame.select(
        F.col("deal_id").cast("string").alias("deal_id"),
        F.col("trade_id").cast("string").alias("trade_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("asset_id").cast("string").alias("asset_id"),
        F.col("collateral_id").cast("string").alias("collateral_id"),
        F.col("netting_set_id").cast("string").alias("netting_set_id"),
        F.col("product_type").cast("string").alias("product_type"),
        F.to_date(F.col("trade_date")).alias("trade_date"),
        F.to_date(F.col("maturity_date")).alias("maturity_date"),
        F.col("currency").cast("string").alias("currency"),
        F.col("notional").cast(DecimalType(20, 2)).alias("notional"),
        F.col("mark_to_market").cast(DecimalType(20, 2)).alias("mark_to_market"),
        F.col("status").cast("string").alias("status"),
        F.to_date(F.col("as_of_date")).alias("as_of_date"),
        F.col("volatility").cast(DecimalType(10, 6)).alias("volatility"),
        F.col("fixed_rate").cast(DecimalType(12, 8)).alias("fixed_rate"),
        F.col("strike").cast(DecimalType(20, 6)).alias("strike"),
        F.col("option_type").cast("string").alias("option_type"),
    ).filter(F.col("deal_id").isNotNull())


def _normalize_customer(frame: DataFrame) -> DataFrame:
    return frame.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("customer_name").cast("string").alias("customer_name"),
        F.col("legal_entity_id").cast("string").alias("legal_entity_id"),
        F.col("rating").cast("string").alias("rating"),
        F.col("country_code").cast("string").alias("country_code"),
        F.col("entity_type").cast("string").alias("entity_type"),
        F.col("active_flag").cast("boolean").alias("active_flag"),
        F.to_date(F.col("as_of_date")).alias("as_of_date"),
    ).filter(F.col("customer_id").isNotNull())


def _normalize_asset(frame: DataFrame) -> DataFrame:
    return frame.select(
        F.col("asset_id").cast("string").alias("asset_id"),
        F.col("isin").cast("string").alias("isin"),
        F.col("asset_class").cast("string").alias("asset_class"),
        F.col("issuer").cast("string").alias("issuer"),
        F.col("currency").cast("string").alias("currency"),
        F.col("market_value").cast(DecimalType(20, 2)).alias("market_value"),
        F.to_date(F.col("valuation_date")).alias("valuation_date"),
    ).filter(F.col("asset_id").isNotNull())


def _normalize_collateral(frame: DataFrame) -> DataFrame:
    return frame.select(
        F.col("collateral_id").cast("string").alias("collateral_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("asset_id").cast("string").alias("asset_id"),
        F.col("agreement_id").cast("string").alias("agreement_id"),
        F.col("collateral_type").cast("string").alias("collateral_type"),
        F.col("quantity").cast(DecimalType(20, 6)).alias("quantity"),
        F.col("market_value").cast(DecimalType(20, 2)).alias("market_value"),
        F.col("currency").cast("string").alias("currency"),
        F.to_date(F.col("valuation_date")).alias("valuation_date"),
    ).filter(F.col("collateral_id").isNotNull())


def _normalize_for_topic(topic: str, frame: DataFrame) -> DataFrame:
    if topic == "risk.deals.ingest":
        return _normalize_deals(frame)
    if topic == "risk.customer.ingest":
        return _normalize_customer(frame)
    if topic == "risk.asset.ingest":
        return _normalize_asset(frame)
    if topic == "risk.collateral.ingest":
        return _normalize_collateral(frame)
    return frame.limit(0)


def _max_date_string(frame: DataFrame, date_column: str) -> str | None:
    if date_column not in frame.columns:
        return None
    value = frame.select(F.max(F.col(date_column)).alias("d")).collect()[0]["d"]
    return value.isoformat() if value else None


def _topic_date_column(topic: str) -> str:
    if topic in {"risk.deals.ingest", "risk.customer.ingest"}:
        return "as_of_date"
    return "valuation_date"


def _trigger_pipeline(spark: SparkSession, entity_dates: dict[str, str]) -> None:
    """Publish one trigger event per entity touched by the micro-batch."""
    rows = [
        (entity, build_trigger_payload(entity, as_of_date))
        for entity, as_of_date in sorted(entity_dates.items())
    ]
    (
        spark.createDataFrame(rows, ["key", "value"])
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TRIGGER_TOPIC)
        .save()
    )


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    parsed = (
        batch_df.select(
            F.col("topic").cast("string").alias("topic"),
            F.col("value").cast("string").alias("value"),
        )
    )

    rows_written = 0
    entity_dates: dict[str, str] = {}

    for topic in INGEST_TOPICS:
        topic_rows = parsed.filter(F.col("topic") == F.lit(topic))
        if topic_rows.rdd.isEmpty():
            continue

        schema = TOPIC_SCHEMA_MAP[topic]
        decoded = topic_rows.select(F.from_json(F.col("value"), schema).alias("d")).select("d.*")
        normalized = _normalize_for_topic(topic, decoded)
        if normalized.rdd.isEmpty():
            continue

        normalized.writeTo(TOPIC_TABLE_MAP[topic]).append()
        rows_written += normalized.count()

        date_value = _max_date_string(normalized, _topic_date_column(topic))
        entity_dates[TOPIC_ENTITY_MAP[topic]] = date_value or date.today().isoformat()

    if rows_written == 0:
        return

    _trigger_pipeline(batch_df.sparkSession, entity_dates)
    print(f"[kafka-entity-stream] batch={batch_id} rows={rows_written} entities={entity_dates}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("risk-analytics-kafka-entity-stream")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(INGEST_TOPICS))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
