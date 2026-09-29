"""Tests for the local Bronze ingestion layer."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks_lakehouse.bronze.ingest import (
    DATASET_SCHEMAS,
    ingest_all_datasets,
)
from databricks_lakehouse.spark import get_spark_session

SOURCE_DIR = Path("data/sample")

EXPECTED_COUNTS = {
    "customers": 500,
    "products": 100,
    "orders": 2_000,
    "order_items": 5_930,
    "payments": 2_000,
    "shipments": 1_814,
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Provide a Spark session for Bronze tests."""
    session = get_spark_session("bronze-tests")
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture(scope="module")
def bronze_dir(
    spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """Create an isolated temporary Bronze layer."""
    target_dir = tmp_path_factory.mktemp("bronze")

    ingest_all_datasets(
        spark=spark,
        source_dir=SOURCE_DIR,
        bronze_dir=target_dir,
        batch_id="test-batch-001",
    )

    return target_dir


def test_all_expected_datasets_have_schemas() -> None:
    """Ensure every expected source has an explicit Spark schema."""
    assert set(DATASET_SCHEMAS) == set(EXPECTED_COUNTS)


@pytest.mark.parametrize(
    ("dataset_name", "expected_count"),
    EXPECTED_COUNTS.items(),
)
def test_bronze_preserves_source_counts(
    spark: SparkSession,
    bronze_dir: Path,
    dataset_name: str,
    expected_count: int,
) -> None:
    """Ensure Bronze preserves the number of source records."""
    dataframe = spark.read.parquet(str(bronze_dir / dataset_name))

    assert dataframe.count() == expected_count


def test_bronze_contains_ingestion_metadata(
    spark: SparkSession,
    bronze_dir: Path,
) -> None:
    """Ensure technical ingestion metadata is present."""
    dataframe = spark.read.parquet(str(bronze_dir / "customers"))

    expected_metadata = {
        "_ingestion_timestamp",
        "_source_file",
        "_batch_id",
    }

    assert expected_metadata.issubset(set(dataframe.columns))


def test_bronze_batch_id_is_preserved(
    spark: SparkSession,
    bronze_dir: Path,
) -> None:
    """Ensure all records receive the requested batch identifier."""
    dataframe = spark.read.parquet(str(bronze_dir / "customers"))

    batch_ids = {row["_batch_id"] for row in dataframe.select("_batch_id").distinct().collect()}

    assert batch_ids == {"test-batch-001"}


def test_bronze_source_file_is_recorded(
    spark: SparkSession,
    bronze_dir: Path,
) -> None:
    """Ensure source lineage information is populated."""
    dataframe = spark.read.parquet(str(bronze_dir / "customers"))

    missing_source_files = dataframe.filter(
        F.col("_source_file").isNull() | (F.col("_source_file") == "")
    ).count()

    assert missing_source_files == 0


def test_bronze_preserves_invalid_customer_emails(
    spark: SparkSession,
    bronze_dir: Path,
) -> None:
    """Ensure Bronze does not silently clean source quality issues."""
    dataframe = spark.read.parquet(str(bronze_dir / "customers"))

    invalid_count = dataframe.filter(
        F.col("email").isNull() | (F.trim(F.col("email")) == "")
    ).count()

    assert invalid_count == 5


def test_bronze_preserves_invalid_order_item_quantities(
    spark: SparkSession,
    bronze_dir: Path,
) -> None:
    """Ensure invalid quantities remain available for Silver processing."""
    dataframe = spark.read.parquet(str(bronze_dir / "order_items"))

    invalid_count = dataframe.filter(F.col("quantity") <= 0).count()

    assert invalid_count == 59
