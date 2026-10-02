"""Pure PySpark transformations used by the declarative pipeline.

Keeping the logic here (not in the pipeline file) means it can be unit tested locally
with a plain Spark session, while the pipeline file stays a thin, declarative layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macro_lakehouse.sources.treasury import sanitize_column

LINEAGE_COLUMNS = ["_source_file", "_file_modified_at", "_ingested_at"]


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