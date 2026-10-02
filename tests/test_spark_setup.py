from pyspark.sql import functions as F


def test_spark_reads_sample_csv(spark, sample_csv_path):
    df = spark.read.option("header", True).csv(sample_csv_path)
    assert df.count() == 3
    assert "1.5 Month" in df.columns
    # The dot must be escaped with backticks, or Spark reads it as a struct field
    assert df.where(F.col("`1.5 Month`").isNull()).count() == 2