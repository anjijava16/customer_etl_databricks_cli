"""Bronze layer: land raw CSV data as-is, plus ingestion metadata."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("total_spend", DoubleType(), True),
    ]
)


def read_raw_customers(spark: SparkSession, path: str) -> DataFrame:
    """Read the raw CSV with an explicit schema (never infer in production)."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(CUSTOMER_SCHEMA)
        .load(path)
    )


def write_bronze(df: DataFrame, table: str) -> None:
    """Append raw rows with ingestion metadata. Bronze is append-only."""
    (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .write.mode("overwrite")  # overwrite keeps this demo idempotent; real feeds append
        .saveAsTable(table)
    )
