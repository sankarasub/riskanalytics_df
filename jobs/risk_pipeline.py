"""Domain transformations for Risk Analytics exposure aggregation; no orchestration concerns."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from risk_analytics.config import load_config


def calculate_risk_metrics(spark, as_of_date: str, risk_run_id: str, source_branch: str) -> DataFrame:
    """Build transparent exposure, PFE, and VaR measures for one reporting date.

    The calculation intentionally keeps each financial assumption explicit: only
    positive mark-to-market contributes to gross exposure, netting occurs at the
    customer/netting-set level, collateral is discounted by asset-class haircut,
    and configured coefficients drive PFE and VaR. This makes the demo easy to
    review and gives the output a clear audit trail.
    """
    cfg = load_config()["risk"]
    trades = spark.table("nessie.risk_analytics.trades_canonical").filter((F.col("as_of_date") == F.lit(as_of_date)) & (F.col("status") == "ACTIVE"))
    products = spark.table("nessie.risk_analytics.trade_product_canonical").select("trade_id", "volatility")
    # Product volatility enriches trade exposure while preserving trades that do
    # not have a matching product record; configuration supplies their default.
    netted = (trades.join(products, "trade_id", "left")
        .withColumn("trade_exposure", F.greatest(F.col("mark_to_market"), F.lit(0)))
        .groupBy("customer_id", "netting_set_id")
        .agg(F.sum("trade_exposure").alias("gross_exposure"), F.greatest(F.sum("mark_to_market"), F.lit(0)).alias("netting_exposure"), F.avg(F.coalesce("volatility", F.lit(cfg["default_volatility"]))).alias("volatility")))
    # Build the haircut table in Spark so the rule is evaluated distributively.
    haircuts = F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in cfg["collateral_haircuts"].items()), []))
    collateral = (spark.table("nessie.risk_analytics.collateral_canonical").filter(F.col("valuation_date") == F.lit(as_of_date))
        .join(spark.table("nessie.risk_analytics.asset_canonical").select("asset_id", "asset_class"), "asset_id")
        .withColumn("haircut", F.coalesce(haircuts[F.col("asset_class")], F.lit(cfg["collateral_haircuts"]["OTHER"])))
        .groupBy("customer_id").agg(F.sum(F.col("market_value") * (1 - F.col("haircut"))).alias("collateral_value_after_haircut")))
    # A left join retains exposure even when no eligible collateral is present.
    return (netted.join(collateral, "customer_id", "left").fillna({"collateral_value_after_haircut": 0})
        .withColumn("pfe", F.greatest(F.col("netting_exposure") * F.lit(cfg["pfe_multiplier"]) - F.col("collateral_value_after_haircut"), F.lit(0)))
        .withColumn("var", F.col("netting_exposure") * F.col("volatility") * F.lit(cfg["var_confidence_z_score"]))
        .select(F.lit(risk_run_id).alias("risk_run_id"), F.lit(as_of_date).cast("date").alias("as_of_date"), "customer_id", "netting_set_id", "gross_exposure", "netting_exposure", "collateral_value_after_haircut", "pfe", "var", F.current_timestamp().alias("calculation_timestamp"), F.lit(source_branch).alias("source_branch")))

