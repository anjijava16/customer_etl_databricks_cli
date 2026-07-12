"""Silver/gold transformations as pure DataFrame -> DataFrame functions.

Keeping these free of any I/O makes them trivially unit-testable
with a local SparkSession (see tests/test_transform.py).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_customers(df: DataFrame) -> DataFrame:
    """Silver rules: dedupe, drop invalid spend, normalize fields."""
    return (
        df.dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("total_spend") >= 0)
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn(
            "has_email",
            F.col("email").isNotNull() & (F.length("email") > 3),
        )
        .withColumn("country", F.upper(F.col("country")))
    )


def spend_by_country(df: DataFrame) -> DataFrame:
    """Gold: business-ready aggregate."""
    return (
        df.groupBy("country")
        .agg(
            F.count("*").alias("customer_count"),
            F.round(F.sum("total_spend"), 2).alias("total_spend"),
            F.round(F.avg("total_spend"), 2).alias("avg_spend"),
        )
        .orderBy(F.col("total_spend").desc())
    )
