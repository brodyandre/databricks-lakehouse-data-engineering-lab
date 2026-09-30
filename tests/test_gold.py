"""Tests for the Gold analytical layer."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks_lakehouse.gold.process import process_gold
from databricks_lakehouse.spark import get_spark_session

SILVER_DIR = Path("data/silver")

EXPECTED_GOLD_COUNTS = {
    "order_revenue": 1_598,
    "product_revenue": 100,
    "category_revenue": 6,
    "customer_revenue": 476,
    "business_kpis": 1,
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Provide a Spark session for Gold tests."""
    session = get_spark_session("gold-tests")
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture(scope="module")
def gold_dir(
    spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """Create isolated Gold outputs."""
    target_dir = tmp_path_factory.mktemp("gold")

    process_gold(
        spark=spark,
        silver_dir=SILVER_DIR,
        gold_dir=target_dir,
    )

    return target_dir


@pytest.mark.parametrize(
    ("dataset_name", "expected_count"),
    EXPECTED_GOLD_COUNTS.items(),
)
def test_gold_expected_counts(
    spark: SparkSession,
    gold_dir: Path,
    dataset_name: str,
    expected_count: int,
) -> None:
    """Ensure Gold datasets contain the expected record counts."""
    dataframe = spark.read.parquet(str(gold_dir / dataset_name))

    assert dataframe.count() == expected_count


def test_order_revenue_contains_only_commercial_statuses(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure order revenue contains only completed or shipped orders."""
    dataframe = spark.read.parquet(str(gold_dir / "order_revenue"))

    invalid_count = dataframe.filter(~F.col("order_status").isin("completed", "shipped")).count()

    assert invalid_count == 0


def test_order_revenue_has_positive_revenue(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure Gold orders contain positive revenue."""
    dataframe = spark.read.parquet(str(gold_dir / "order_revenue"))

    invalid_count = dataframe.filter(
        F.col("order_revenue").isNull() | (F.col("order_revenue") <= 0)
    ).count()

    assert invalid_count == 0


def test_product_revenue_has_all_products(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure product revenue contains all products represented in Gold."""
    dataframe = spark.read.parquet(str(gold_dir / "product_revenue"))

    assert dataframe.select("product_id").distinct().count() == 100


def test_category_revenue_has_all_categories(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure category revenue contains all expected categories."""
    dataframe = spark.read.parquet(str(gold_dir / "category_revenue"))

    categories = {row["category"] for row in dataframe.select("category").collect()}

    assert categories == {
        "electronics",
        "home",
        "books",
        "sports",
        "beauty",
        "fashion",
    }


def test_customer_revenue_has_valid_customer_count(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure customer revenue contains the expected active customers."""
    dataframe = spark.read.parquet(str(gold_dir / "customer_revenue"))

    assert dataframe.select("customer_id").distinct().count() == 476


def test_customer_revenue_has_no_blank_emails(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure rejected Silver customers do not reappear in Gold."""
    dataframe = spark.read.parquet(str(gold_dir / "customer_revenue"))

    invalid_count = dataframe.filter(
        F.col("email").isNull() | (F.trim(F.col("email")) == "")
    ).count()

    assert invalid_count == 0


def test_business_kpis_are_consistent_with_customer_revenue(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure KPI active customers match customer Gold population."""
    kpis = spark.read.parquet(str(gold_dir / "business_kpis"))
    customers = spark.read.parquet(str(gold_dir / "customer_revenue"))

    active_customers = kpis.first()["active_customers"]
    customer_count = customers.select("customer_id").distinct().count()

    assert active_customers == customer_count


def test_business_kpis_match_expected_values(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure business KPIs match deterministic source data."""
    dataframe = spark.read.parquet(str(gold_dir / "business_kpis"))

    row = dataframe.first()

    assert row is not None
    assert row["total_orders"] == 1_598
    assert row["active_customers"] == 476
    assert row["total_revenue"] == pytest.approx(17_120_904.23)
    assert row["average_order_value"] == pytest.approx(10_713.957590738426)
    assert row["total_items_sold"] == 14_039


def test_category_revenue_matches_total_revenue(
    spark: SparkSession,
    gold_dir: Path,
) -> None:
    """Ensure category revenue reconciles with overall Gold revenue."""
    categories = spark.read.parquet(str(gold_dir / "category_revenue"))
    kpis = spark.read.parquet(str(gold_dir / "business_kpis"))

    category_total = categories.agg(F.sum("category_revenue").alias("total")).first()["total"]

    total_revenue = kpis.first()["total_revenue"]

    assert category_total == pytest.approx(total_revenue)
