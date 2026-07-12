"""Entry point invoked by the Databricks python_wheel_task.

Parameters arrive from resources/customer_etl_job.yml, where the bundle
substitutes the workspace file path, catalog, and schema at deploy time.
"""

import argparse

from pyspark.sql import SparkSession

from customer_etl.ingestion import read_raw_customers, write_bronze
from customer_etl.transform import clean_customers, spend_by_country
from customer_etl.validation import expect_non_empty, expect_unique_key


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Customer ETL (bronze->silver->gold)")
    parser.add_argument("--data-path", required=True, help="Path to input CSV")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="customer_etl")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()  # attach to the cluster's session

    catalog, schema = args.catalog, args.schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    bronze = f"`{catalog}`.`{schema}`.customers_bronze"
    silver = f"`{catalog}`.`{schema}`.customers_silver"
    gold = f"`{catalog}`.`{schema}`.spend_by_country_gold"

    # ---- Bronze: land raw data ----
    raw = read_raw_customers(spark, args.data_path)
    expect_non_empty(raw, "raw input")
    write_bronze(raw, bronze)
    print(f"[bronze] wrote {spark.table(bronze).count()} rows -> {bronze}")

    # ---- Silver: clean + validate ----
    cleaned = clean_customers(spark.table(bronze))
    expect_non_empty(cleaned, "silver")
    expect_unique_key(cleaned, "customer_id", "silver")
    cleaned.write.mode("overwrite").saveAsTable(silver)
    print(f"[silver] wrote {spark.table(silver).count()} rows -> {silver}")

    # ---- Gold: aggregate ----
    agg = spend_by_country(spark.table(silver))
    agg.write.mode("overwrite").saveAsTable(gold)
    print(f"[gold]   wrote {spark.table(gold).count()} rows -> {gold}")

    spark.table(gold).show(truncate=False)
    print("Customer ETL completed successfully.")


if __name__ == "__main__":
    main()
