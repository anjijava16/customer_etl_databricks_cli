"""Minimal fail-fast validations between layers."""

from pyspark.sql import DataFrame


class DataQualityError(Exception):
    """Raised when a layer fails validation — fails the Databricks task."""


def expect_non_empty(df: DataFrame, name: str) -> None:
    if df.limit(1).count() == 0:
        raise DataQualityError(f"{name} is empty — aborting pipeline")


def expect_unique_key(df: DataFrame, key: str, name: str) -> None:
    total = df.count()
    distinct = df.select(key).distinct().count()
    if total != distinct:
        raise DataQualityError(
            f"{name}: duplicate values in key '{key}' ({total} rows, {distinct} distinct)"
        )
