from datetime import date

import pytest
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


# ---------- silver ----------
def test_to_long_one_row_per_date_and_maturity(bronze_df):
    # 3 dates x 14 tenors = 42 values, minus 2 blank "1.5 Month" values
    assert t.to_long(bronze_df).count() == 40


def test_to_long_types(bronze_df):
    types = dict(t.to_long(bronze_df).dtypes)
    assert types["curve_date"] == "date"
    assert types["maturity_months"] == "double"
    assert types["yield_pct"] == "double"


def test_to_long_values_and_labels(bronze_df):
    row = (
        t.to_long(bronze_df)
        .where((F.col("curve_date") == date(2025, 3, 6)) & (F.col("maturity_months") == 120.0))
        .first()
    )
    assert row.yield_pct == 4.28
    assert row.maturity_label == "10Y"


def test_to_long_drops_blank_values(bronze_df):
    blanks = t.to_long(bronze_df).where(
        (F.col("maturity_months") == 1.5) & (F.col("curve_date") < date(2025, 3, 6))
    )
    assert blanks.count() == 0


def test_to_long_keeps_lineage(bronze_df):
    assert set(t.LINEAGE_COLUMNS) <= set(t.to_long(bronze_df).columns)


def test_to_long_rejects_data_without_tenors(spark):
    df = spark.createDataFrame([("03/06/2025",)], ["date"])
    with pytest.raises(ValueError, match="No tenor columns"):
        t.to_long(df)
