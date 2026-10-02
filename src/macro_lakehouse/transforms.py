"""Pure PySpark transformations used by the declarative pipeline.

Keeping the logic here (not in the pipeline file) means it can be unit tested locally
with a plain Spark session, while the pipeline file stays a thin, declarative layer.
"""

from datetime import date
from itertools import chain

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from macro_lakehouse.sources.treasury import sanitize_column, tenor_label, tenor_months

LINEAGE_COLUMNS = ["_source_file", "_file_modified_at", "_ingested_at"]

KEY_COLUMNS = ["curve_date", "maturity_months"]


# ---------------------------------------------------------------- bronze
def standardise_raw(raw: DataFrame) -> DataFrame:
    """Rename raw CSV headers to Delta-safe names and add lineage columns.

    Works for batch and streaming (Auto Loader) DataFrames. Columns that already
    start with "_" (such as Auto Loader's `_rescued_data`) keep their names.
    Values are left as strings: bronze stores data exactly as received.
    """
    renamed = raw.toDF(*[c if c.startswith("_") else sanitize_column(c) for c in raw.columns])
    return (
        renamed.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------------------------------------------------------------- silver
def to_long(bronze: DataFrame) -> DataFrame:
    """Unpivot the wide Treasury layout into one typed row per (date, maturity).

    Blank values are dropped: Treasury leaves a tenor empty for dates before it existed.
    """
    tenor_cols = [c for c in bronze.columns if tenor_months(c) is not None]
    if not tenor_cols:
        raise ValueError(f"No tenor columns found in bronze data: {bronze.columns}")

    months_map = F.create_map(
        *chain.from_iterable((F.lit(c), F.lit(tenor_months(c))) for c in tenor_cols)
    )
    labels_map = F.create_map(
        *chain.from_iterable((F.lit(c), F.lit(tenor_label(tenor_months(c)))) for c in tenor_cols)
    )

    long_df = bronze.select("date", *LINEAGE_COLUMNS, *tenor_cols).unpivot(
        ids=["date", *LINEAGE_COLUMNS],
        values=tenor_cols,
        variableColumnName="tenor_column",
        valueColumnName="yield_raw",
    )
    return (
        long_df.withColumn("yield_raw", F.trim("yield_raw"))
        .where(F.col("yield_raw").isNotNull() & (F.col("yield_raw") != ""))
        .select(
            F.to_date("date", "MM/dd/yyyy").alias("curve_date"),
            months_map[F.col("tenor_column")].alias("maturity_months"),
            labels_map[F.col("tenor_column")].alias("maturity_label"),
            F.col("yield_raw").cast("double").alias("yield_pct"),
            *LINEAGE_COLUMNS,
        )
    )


# ---------------------------------------------------------------- point-in-time (SCD type 2)
SEQUENCE_COLUMN = "_file_modified_at"
HISTORY_COLUMNS = ["yield_pct"]  # only a changed yield creates a new version


def change_sequence() -> Column:
    """Ordering AUTO CDC uses to decide which version of a value is newest.

    Every ingest run writes new files, so the file modification time orders revisions.
    In SCD type 2 this value becomes the __START_AT / __END_AT of each version.
    """
    return F.col(SEQUENCE_COLUMN)


def current_rows(scd2: DataFrame) -> DataFrame:
    """Latest version of every key: the rows whose validity has not ended."""
    return scd2.where(F.col("__END_AT").isNull())


def as_of(scd2: DataFrame, known_at) -> DataFrame:
    """Each key exactly as it was known at `known_at` (point-in-time view).

    Prevents look-ahead bias: a backtest only sees values that had been published
    at the time, not later revisions.
    """
    ts = F.lit(known_at).cast("timestamp")
    return scd2.where(
        (F.col("__START_AT") <= ts) & (F.col("__END_AT").isNull() | (F.col("__END_AT") > ts))
    )


# ---------------------------------------------------------------- gold
KEY_TENORS = {3.0: "y_3m", 24.0: "y_2y", 120.0: "y_10y", 360.0: "y_30y"}
FLAT_THRESHOLD_PCT = 0.10


def build_curve_metrics(silver: DataFrame) -> DataFrame:
    """One row per date with the key tenors and the classic recession spreads."""
    wide = (
        silver.where(F.col("maturity_months").isin(list(KEY_TENORS)))
        .groupBy("curve_date")
        .pivot("maturity_months", list(KEY_TENORS))
        .agg(F.first("yield_pct"))
    )
    for months, name in KEY_TENORS.items():
        wide = wide.withColumnRenamed(str(months), name)

    by_date = Window.orderBy("curve_date")
    return (
        wide.withColumn("spread_10y_2y", F.round(F.col("y_10y") - F.col("y_2y"), 4))
        .withColumn("spread_10y_3m", F.round(F.col("y_10y") - F.col("y_3m"), 4))
        .withColumn(
            "curve_shape",
            F.when(F.col("spread_10y_2y").isNull(), None)
            .when(F.col("spread_10y_2y") < 0, "inverted")
            .when(F.abs("spread_10y_2y") < FLAT_THRESHOLD_PCT, "flat")
            .otherwise("normal"),
        )
        .withColumn("change_1d_10y", F.round(F.col("y_10y") - F.lag("y_10y").over(by_date), 4))
    )


# ---------------------------------------------------------------- table-level quality
def table_health(df: DataFrame, today: date | None = None) -> DataFrame:
    """One-row summary for table-level checks: row count, duplicate keys, data age.

    Row-level rules (nulls, ranges) are pipeline expectations; these checks need a
    whole-table view, so they are computed here and checked by expectations on this row.
    """
    today_col = F.lit(today) if today else F.current_date()
    duplicates = (
        df.groupBy(*KEY_COLUMNS)
        .count()
        .where("count > 1")
        .agg(F.count("*").alias("duplicate_keys"))
    )
    stats = df.agg(
        F.count("*").alias("row_count"),
        F.max("curve_date").alias("latest_curve_date"),
    )
    return stats.crossJoin(duplicates).withColumn(
        "age_days", F.datediff(today_col, F.col("latest_curve_date"))
    )
