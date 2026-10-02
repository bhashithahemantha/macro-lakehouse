from pyspark.sql import functions as F

from macro_lakehouse import transforms as t


# ---------- bronze ----------
def test_standardise_renames_headers(bronze_df):
    assert {"date", "t_1_mo", "t_1_5_month", "t_10_yr"} <= set(bronze_df.columns)
    assert bronze_df.count() == 3


def test_standardise_keeps_raw_strings(bronze_df):
    assert dict(bronze_df.dtypes)["t_10_yr"] == "string"


def test_standardise_adds_lineage(bronze_df):
    assert set(t.LINEAGE_COLUMNS) <= set(bronze_df.columns)
    row = bronze_df.first()
    assert row["_source_file"].endswith("treasury_sample.csv")
    assert row["_file_modified_at"] is not None
    assert row["_ingested_at"] is not None


def test_standardise_keeps_system_columns(spark):
    raw = spark.createDataFrame([("4.28", None)], "`10 Yr` string, _rescued_data string")
    raw = raw.withColumn(
        "_metadata",
        F.struct(
            F.lit("f.csv").alias("file_path"),
            F.current_timestamp().alias("file_modification_time"),
        ),
    )
    assert {"t_10_yr", "_rescued_data"} <= set(t.standardise_raw(raw).columns)