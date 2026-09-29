"""Spark session factory for local development."""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "databricks-lakehouse-data-engineering-lab") -> SparkSession:
    """Create or reuse a local Spark session."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
