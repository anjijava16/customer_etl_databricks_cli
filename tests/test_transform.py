"""Unit tests for pure transform functions — run locally, no Databricks needed.

    pip install -e ".[dev]"
    pytest tests/ -v
"""

import datetime

import pytest
from pyspark.sql import SparkSession

from customer_etl.transform import clean_customers, spend_by_country

COLUMNS = ["customer_id", "name", "email", "country", "signup_date", "total_spend"]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("customer-etl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def sample_df(spark):
    rows = [
        (1001, "Ravi Kumar", "Ravi.Kumar@Example.com ", "in", datetime.date(2024, 3, 15), 1250.50),
        (1002, "Sarah Chen", "sarah.chen@example.com", "US", datetime.date(2024, 5, 2), 890.00),
        (1002, "Sarah Chen", "sarah.chen@example.com", "US", datetime.date(2024, 5, 2), 890.00),  # dup
        (1003, "Luis Garcia", "luis.garcia@example.com", "MX", datetime.date(2024, 6, 9), -99.99),  # bad spend
        (1004, "Anna Kowalski", None, "PL", datetime.date(2024, 2, 28), 675.00),  # no email
    ]
    return spark.createDataFrame(rows, COLUMNS)


def test_clean_customers_dedupes_and_filters(sample_df):
    result = clean_customers(sample_df)
    assert result.count() == 3  # dup removed, negative spend removed

    ids = {r.customer_id for r in result.collect()}
    assert ids == {1001, 1002, 1004}


def test_clean_customers_normalizes_email_and_country(sample_df):
    result = {r.customer_id: r for r in clean_customers(sample_df).collect()}
    assert result[1001].email == "ravi.kumar@example.com"
    assert result[1001].country == "IN"
    assert result[1001].has_email is True
    assert result[1004].has_email is False


def test_spend_by_country_aggregates(sample_df):
    gold = spend_by_country(clean_customers(sample_df))
    by_country = {r.country: r for r in gold.collect()}

    assert by_country["IN"].customer_count == 1
    assert by_country["IN"].total_spend == 1250.50
    assert set(by_country) == {"IN", "US", "PL"}
