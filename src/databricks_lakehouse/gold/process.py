"""Gold processing pipeline."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from databricks_lakehouse.gold.analytics import (
    build_business_kpis,
    build_category_revenue,
    build_customer_revenue,
    build_order_revenue,
    build_product_revenue,
)


def write_delta(dataframe: DataFrame, target_path: Path) -> None:
    """Write a DataFrame to Delta Lake."""
    dataframe.write.mode("overwrite").format("delta").save(str(target_path))


def read_silver_delta(
    spark: SparkSession,
    dataset_path: Path,
) -> DataFrame:
    """Read a Silver dataset stored in Delta format."""
    return spark.read.format("delta").load(str(dataset_path))


def process_gold(
    spark: SparkSession,
    silver_dir: Path,
    gold_dir: Path,
) -> dict[str, int]:
    """Process Silver datasets into Gold analytical datasets."""
    customers = read_silver_delta(
        spark=spark,
        dataset_path=silver_dir / "customers",
    )

    products = read_silver_delta(
        spark=spark,
        dataset_path=silver_dir / "products",
    )

    orders = read_silver_delta(
        spark=spark,
        dataset_path=silver_dir / "orders",
    )

    order_items = read_silver_delta(
        spark=spark,
        dataset_path=silver_dir / "order_items",
    )

    order_revenue = build_order_revenue(
        orders=orders,
        order_items=order_items,
        customers=customers,
    )

    product_revenue = build_product_revenue(
        orders=orders,
        order_items=order_items,
        products=products,
        customers=customers,
    )

    category_revenue = build_category_revenue(
        product_revenue=product_revenue,
    )

    customer_revenue = build_customer_revenue(
        customers=customers,
        order_revenue=order_revenue,
    )

    business_kpis = build_business_kpis(
        order_revenue=order_revenue,
    )

    outputs = {
        "order_revenue": order_revenue,
        "product_revenue": product_revenue,
        "category_revenue": category_revenue,
        "customer_revenue": customer_revenue,
        "business_kpis": business_kpis,
    }

    counts: dict[str, int] = {}

    for dataset_name, dataframe in outputs.items():
        write_delta(
            dataframe=dataframe,
            target_path=gold_dir / dataset_name,
        )

        counts[dataset_name] = dataframe.count()

    return counts
