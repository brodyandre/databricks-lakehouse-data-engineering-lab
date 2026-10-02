"""Tests for the Silver data quality layer."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks_lakehouse.silver.process import (
    metrics_to_dataframe,
    process_all_datasets,
)
from databricks_lakehouse.spark import get_spark_session

BRONZE_DIR = Path("data/bronze")

EXPECTED_SILVER_COUNTS = {
    "customers": 495,
    "products": 100,
    "orders": 2_000,
    "order_items": 5_871,
    "payments": 2_000,
    "shipments": 1_814,
}

EXPECTED_REJECTED_COUNTS = {
    "customers": 5,
    "order_items": 59,
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Provide a Spark session for Silver tests."""
    session = get_spark_session("silver-tests")
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture(scope="module")
def silver_outputs(
    spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, Path]:
    """Create isolated Silver, rejected and quality outputs."""
    root_dir = tmp_path_factory.mktemp("silver")

    silver_dir = root_dir / "silver"
    rejected_dir = root_dir / "rejected"
    quality_dir = root_dir / "quality"

    metrics = process_all_datasets(
        spark=spark,
        bronze_dir=BRONZE_DIR,
        silver_dir=silver_dir,
        rejected_dir=rejected_dir,
        batch_id="test-silver-001",
    )

    metrics_df = metrics_to_dataframe(
        spark=spark,
        metrics=metrics,
    )

    metrics_df.write.mode("overwrite").format("parquet").save(str(quality_dir))

    return {
        "silver": silver_dir,
        "rejected": rejected_dir,
        "quality": quality_dir,
    }


@pytest.mark.parametrize(
    ("dataset_name", "expected_count"),
    EXPECTED_SILVER_COUNTS.items(),
)
def test_silver_expected_counts(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
    dataset_name: str,
    expected_count: int,
) -> None:
    """Ensure Silver datasets contain the expected valid record counts."""
    dataframe = spark.read.format("delta").load(str(silver_outputs["silver"] / dataset_name))

    assert dataframe.count() == expected_count


@pytest.mark.parametrize(
    ("dataset_name", "expected_count"),
    EXPECTED_REJECTED_COUNTS.items(),
)
def test_rejected_expected_counts(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
    dataset_name: str,
    expected_count: int,
) -> None:
    """Ensure rejected datasets contain expected invalid records."""
    dataframe = spark.read.parquet(str(silver_outputs["rejected"] / dataset_name))

    assert dataframe.count() == expected_count


def test_silver_customers_have_no_blank_emails(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure Silver customers contain no blank email values."""
    dataframe = spark.read.format("delta").load(str(silver_outputs["silver"] / "customers"))

    invalid_count = dataframe.filter(
        F.col("email").isNull() | (F.trim(F.col("email")) == "")
    ).count()

    assert invalid_count == 0


def test_silver_order_items_have_valid_quantities(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure Silver order items contain only positive quantities."""
    dataframe = spark.read.format("delta").load(str(silver_outputs["silver"] / "order_items"))

    invalid_count = dataframe.filter(F.col("quantity").isNull() | (F.col("quantity") <= 0)).count()

    assert invalid_count == 0


def test_rejected_customers_include_quality_reason(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure rejected customers include a data quality reason."""
    dataframe = spark.read.parquet(str(silver_outputs["rejected"] / "customers"))

    missing_reason_count = dataframe.filter(
        F.col("_dq_reason").isNull() | (F.trim(F.col("_dq_reason")) == "")
    ).count()

    assert missing_reason_count == 0


def test_rejected_order_items_include_quality_reason(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure rejected order items include a data quality reason."""
    dataframe = spark.read.parquet(str(silver_outputs["rejected"] / "order_items"))

    missing_reason_count = dataframe.filter(
        F.col("_dq_reason").isNull() | (F.trim(F.col("_dq_reason")) == "")
    ).count()

    assert missing_reason_count == 0


def test_quality_metrics_contains_all_datasets(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure quality metrics contain all retail datasets."""
    dataframe = spark.read.parquet(str(silver_outputs["quality"]))

    datasets = {row["dataset"] for row in dataframe.select("dataset").collect()}

    assert datasets == set(EXPECTED_SILVER_COUNTS)


def test_quality_metrics_use_expected_batch_id(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure quality metrics retain the processing batch id."""
    dataframe = spark.read.parquet(str(silver_outputs["quality"]))

    batch_ids = {row["batch_id"] for row in dataframe.select("batch_id").distinct().collect()}

    assert batch_ids == {"test-silver-001"}


def test_customer_quality_metrics_are_correct(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure customer quality metrics are accurate."""
    dataframe = spark.read.parquet(str(silver_outputs["quality"]))

    row = dataframe.filter(F.col("dataset") == "customers").first()

    assert row is not None
    assert row["input_records"] == 500
    assert row["valid_records"] == 495
    assert row["rejected_records"] == 5
    assert row["quality_rate"] == pytest.approx(0.99)


def test_order_item_quality_metrics_are_correct(
    spark: SparkSession,
    silver_outputs: dict[str, Path],
) -> None:
    """Ensure order item quality metrics are accurate."""
    dataframe = spark.read.parquet(str(silver_outputs["quality"]))

    row = dataframe.filter(F.col("dataset") == "order_items").first()

    assert row is not None
    assert row["input_records"] == 5_930
    assert row["valid_records"] == 5_871
    assert row["rejected_records"] == 59
    assert row["quality_rate"] == pytest.approx(5_871 / 5_930)
