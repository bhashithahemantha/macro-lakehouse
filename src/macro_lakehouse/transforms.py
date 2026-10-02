"""Pure PySpark transformations used by the declarative pipeline.

Keeping the logic here (not in the pipeline file) means it can be unit tested locally
with a plain Spark session, while the pipeline file stays a thin, declarative layer.
"""

from itertools import chain

from pyspark.sql import DataFrame
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
