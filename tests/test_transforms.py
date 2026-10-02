from datetime import date, datetime

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


# ---------- point-in-time (SCD type 2) ----------
SCD2_COLUMNS = ["curve_date", "maturity_months", "yield_pct", "__START_AT", "__END_AT"]
SCD2_ROWS = [
    # The 10Y yield for 6 March was first published as 4.20, then revised to 4.28
    (date(2025, 3, 6), 120.0, 4.20, datetime(2025, 3, 6, 23), datetime(2025, 3, 7, 23)),
    (date(2025, 3, 6), 120.0, 4.28, datetime(2025, 3, 7, 23), None),
]


@pytest.fixture
def history_df(spark):
    return spark.createDataFrame(SCD2_ROWS, SCD2_COLUMNS)


def test_change_sequence_orders_by_file_time(spark):
    df = spark.createDataFrame(
        [(datetime(2025, 3, 6, 7),), (datetime(2025, 3, 7, 7),)], [t.SEQUENCE_COLUMN]
    )
    newest = df.orderBy(t.change_sequence().desc()).first()[0]
    assert newest == datetime(2025, 3, 7, 7)


def test_current_rows_keeps_latest_version(history_df):
    assert [r.yield_pct for r in t.current_rows(history_df).collect()] == [4.28]


def test_as_of_before_revision_sees_original_value(history_df):
    rows = t.as_of(history_df, datetime(2025, 3, 7, 12)).collect()
    assert [r.yield_pct for r in rows] == [4.20]


def test_as_of_after_revision_sees_revised_value(history_df):
    rows = t.as_of(history_df, datetime(2025, 3, 8, 12)).collect()
    assert [r.yield_pct for r in rows] == [4.28]


def test_as_of_before_publication_sees_nothing(history_df):
    assert t.as_of(history_df, datetime(2025, 3, 6, 12)).count() == 0


def test_as_of_at_exact_revision_time_sees_new_value(history_df):
    rows = t.as_of(history_df, datetime(2025, 3, 7, 23)).collect()
    assert [r.yield_pct for r in rows] == [4.28]


# ---------- gold ----------
@pytest.fixture
def gold_by_date(bronze_df):
    rows = t.build_curve_metrics(t.to_long(bronze_df)).collect()
    return {r.curve_date: r for r in rows}


def test_gold_one_row_per_date(gold_by_date):
    assert len(gold_by_date) == 3


def test_gold_key_tenors(gold_by_date):
    day = gold_by_date[date(2025, 3, 6)]
    assert (day.y_3m, day.y_2y, day.y_10y, day.y_30y) == (4.34, 3.97, 4.28, 4.58)


def test_gold_spreads(gold_by_date):
    day = gold_by_date[date(2025, 3, 6)]
    assert day.spread_10y_2y == 0.31  # 4.28 - 3.97
    assert day.spread_10y_3m == -0.06  # 4.28 - 4.34: the 3m-10y curve is inverted


def test_gold_daily_change(gold_by_date):
    assert gold_by_date[date(2025, 3, 6)].change_1d_10y == 0.0  # 4.28 -> 4.28
    assert gold_by_date[date(2025, 3, 5)].change_1d_10y == 0.07  # 4.21 -> 4.28
    assert gold_by_date[date(2025, 3, 4)].change_1d_10y is None  # no earlier day


def test_curve_shape_classification(spark):
    rows = [
        (date(2023, 7, 3), 24.0, 4.90),
        (date(2023, 7, 3), 120.0, 3.80),  # inverted
        (date(2023, 7, 5), 24.0, 4.00),
        (date(2023, 7, 5), 120.0, 4.05),  # flat
        (date(2023, 7, 6), 24.0, 3.50),
        (date(2023, 7, 6), 120.0, 4.20),  # normal
    ]
    df = spark.createDataFrame(rows, ["curve_date", "maturity_months", "yield_pct"])
    shapes = {r.curve_date: r.curve_shape for r in t.build_curve_metrics(df).collect()}
    assert shapes == {
        date(2023, 7, 3): "inverted",
        date(2023, 7, 5): "flat",
        date(2023, 7, 6): "normal",
    }


# ---------- table-level quality ----------
def test_table_health_on_clean_data(bronze_df):
    health = t.table_health(t.to_long(bronze_df), today=date(2025, 3, 10)).first()
    assert health.row_count == 40
    assert health.duplicate_keys == 0
    assert health.latest_curve_date == date(2025, 3, 6)
    assert health.age_days == 4


def test_table_health_detects_duplicates(spark):
    rows = [(date(2025, 3, 6), 120.0)] * 2 + [(date(2025, 3, 6), 24.0)]
    df = spark.createDataFrame(rows, ["curve_date", "maturity_months"])
    assert t.table_health(df, today=date(2025, 3, 6)).first().duplicate_keys == 1


def test_table_health_detects_stale_data(bronze_df):
    health = t.table_health(t.to_long(bronze_df), today=date(2025, 4, 1)).first()
    assert health.age_days == 26
