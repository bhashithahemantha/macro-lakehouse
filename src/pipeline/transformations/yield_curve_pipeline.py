"""Lakeflow Spark Declarative Pipeline: bronze -> silver -> gold for Treasury yield curves.

This file only *declares* datasets and their quality rules. The pipeline engine works out
the dependency graph, runs each flow incrementally, manages checkpoints and records
expectation metrics in the event log. The transformation logic lives in
macro_lakehouse.transforms so it can be unit tested outside the pipeline.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from macro_lakehouse import transforms as t

spark = SparkSession.active()

LANDING_PATH = spark.conf.get("macro.landing_path")
MAX_AGE_DAYS = int(spark.conf.get("macro.max_age_days", "7"))


# ---------------------------------------------------------------- bronze
@dp.table(
    name="bronze_treasury_yields",
    comment="Raw Treasury CSV rows as strings, with file lineage. Append-only audit trail.",
)
def bronze_treasury_yields():
    raw = (
        spark.readStream.format("cloudFiles")  # Auto Loader: only new files are processed
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")  # bronze keeps raw strings
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # new tenors evolve schema
        .load(LANDING_PATH)
    )
    return t.standardise_raw(raw)


# ---------------------------------------------------------------- silver
@dp.temporary_view(name="treasury_yields_long")
@dp.expect_or_fail("valid_curve_date", "curve_date IS NOT NULL")
@dp.expect_or_fail("known_maturity", "maturity_months IS NOT NULL")
@dp.expect_or_drop("yield_in_range", "yield_pct BETWEEN -2 AND 25")
def treasury_yields_long():
    return t.to_long(spark.readStream.table("bronze_treasury_yields"))


dp.create_streaming_table(
    name="silver_treasury_yields_history",
    comment="Every published version per (curve_date, maturity): AUTO CDC SCD type 2.",
    cluster_by=["curve_date", "maturity_months"],
)

dp.create_auto_cdc_flow(
    target="silver_treasury_yields_history",
    source="treasury_yields_long",
    keys=t.KEY_COLUMNS,
    sequence_by=t.change_sequence(),
    stored_as_scd_type=2,
    track_history_column_list=t.HISTORY_COLUMNS,
)


@dp.materialized_view(
    name="silver_treasury_yields",
    comment="Current value per (curve_date, maturity), from the history table.",
    cluster_by=["curve_date"],
)
def silver_treasury_yields():
    return t.current_rows(spark.read.table("silver_treasury_yields_history"))


# ---------------------------------------------------------------- gold
@dp.materialized_view(
    name="gold_yield_curve_daily",
    comment="Daily key tenors, 10y-2y and 10y-3m spreads, curve shape and 10y daily change.",
    cluster_by=["curve_date"],
)
@dp.expect("has_10y_and_2y", "y_10y IS NOT NULL AND y_2y IS NOT NULL")
def gold_yield_curve_daily():
    return t.build_curve_metrics(spark.read.table("silver_treasury_yields"))


# ---------------------------------------------------------------- table-level quality
@dp.materialized_view(
    name="ops_silver_health",
    comment="Table-level checks on silver: fails the update if empty, duplicated or stale.",
)
@dp.expect_or_fail("not_empty", "row_count > 0")
@dp.expect_or_fail("unique_keys", "duplicate_keys = 0")
@dp.expect_or_fail("fresh_data", f"age_days <= {MAX_AGE_DAYS}")
def ops_silver_health():
    return t.table_health(spark.read.table("silver_treasury_yields"))