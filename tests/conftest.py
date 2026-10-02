from pathlib import Path

import pytest
from pyspark.sql import SparkSession

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