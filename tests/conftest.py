from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from macro_lakehouse import transforms

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("macro-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_csv_path() -> str:
    return str(FIXTURES / "treasury_sample.csv")


@pytest.fixture
def bronze_df(spark, sample_csv_path):
    """The sample CSV read like bronze does: header row, no type inference."""
    raw = spark.read.option("header", True).option("inferSchema", False).csv(sample_csv_path)
    return transforms.standardise_raw(raw)
