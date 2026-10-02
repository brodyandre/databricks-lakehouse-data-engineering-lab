"""Silver processing pipeline with data quality outputs."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks_lakehouse.silver.transform import (
    deduplicate_by_key,
    split_customers_by_quality,
    split_order_items_by_quality,
)


def build_quality_metrics(
    dataset_name: str,
    input_count: int,
    valid_count: int,
    rejected_count: int,
    batch_id: str,
) -> dict[str, object]:
    """Build a quality metrics record."""
    quality_rate = valid_count / input_count if input_count else 0.0

    return {
        "dataset": dataset_name,
        "input_records": input_count,
        "valid_records": valid_count,
        "rejected_records": rejected_count,
        "quality_rate": quality_rate,
        "batch_id": batch_id,
    }


def write_parquet(dataframe: DataFrame, target_path: Path) -> None:
    """Write a DataFrame to Parquet."""
    dataframe.write.mode("overwrite").format("parquet").save(str(target_path))


def write_delta(dataframe: DataFrame, target_path: Path) -> None:
    """Write a DataFrame to Delta Lake."""
    dataframe.write.mode("overwrite").format("delta").save(str(target_path))


def read_bronze_delta(
    spark: SparkSession,
    dataset_path: Path,
) -> DataFrame:
    """Read a Bronze dataset stored in Delta format."""
    return spark.read.format("delta").load(str(dataset_path))


def process_customers(
    spark: SparkSession,
    bronze_dir: Path,
    silver_dir: Path,
    rejected_dir: Path,
    batch_id: str,
) -> dict[str, object]:
    """Process customers from Bronze into Silver."""
    dataframe = read_bronze_delta(
        spark=spark,
        dataset_path=bronze_dir / "customers",
    )

    input_count = dataframe.count()

    valid, rejected = split_customers_by_quality(dataframe)

    valid = deduplicate_by_key(valid, "customer_id")

    valid_count = valid.count()
    rejected_count = rejected.count()

    write_delta(valid, silver_dir / "customers")
    write_parquet(rejected, rejected_dir / "customers")

    return build_quality_metrics(
        dataset_name="customers",
        input_count=input_count,
        valid_count=valid_count,
        rejected_count=rejected_count,
        batch_id=batch_id,
    )


def process_order_items(
    spark: SparkSession,
    bronze_dir: Path,
    silver_dir: Path,
    rejected_dir: Path,
    batch_id: str,
) -> dict[str, object]:
    """Process order items from Bronze into Silver."""
    dataframe = read_bronze_delta(
        spark=spark,
        dataset_path=bronze_dir / "order_items",
    )

    input_count = dataframe.count()

    valid, rejected = split_order_items_by_quality(dataframe)

    valid = deduplicate_by_key(valid, "order_item_id")

    valid_count = valid.count()
    rejected_count = rejected.count()

    write_delta(valid, silver_dir / "order_items")
    write_parquet(rejected, rejected_dir / "order_items")

    return build_quality_metrics(
        dataset_name="order_items",
        input_count=input_count,
        valid_count=valid_count,
        rejected_count=rejected_count,
        batch_id=batch_id,
    )


def process_passthrough_dataset(
    spark: SparkSession,
    dataset_name: str,
    key_column: str,
    bronze_dir: Path,
    silver_dir: Path,
    batch_id: str,
) -> dict[str, object]:
    """Process datasets without custom rejection rules."""
    dataframe = read_bronze_delta(
        spark=spark,
        dataset_path=bronze_dir / dataset_name,
    )

    input_count = dataframe.count()

    valid = deduplicate_by_key(dataframe, key_column)

    valid_count = valid.count()

    write_delta(valid, silver_dir / dataset_name)

    return build_quality_metrics(
        dataset_name=dataset_name,
        input_count=input_count,
        valid_count=valid_count,
        rejected_count=0,
        batch_id=batch_id,
    )


def process_all_datasets(
    spark: SparkSession,
    bronze_dir: Path,
    silver_dir: Path,
    rejected_dir: Path,
    batch_id: str,
) -> list[dict[str, object]]:
    """Process all Bronze datasets into Silver."""
    metrics = [
        process_customers(
            spark=spark,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            rejected_dir=rejected_dir,
            batch_id=batch_id,
        ),
        process_order_items(
            spark=spark,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            rejected_dir=rejected_dir,
            batch_id=batch_id,
        ),
    ]

    passthrough_datasets = {
        "products": "product_id",
        "orders": "order_id",
        "payments": "payment_id",
        "shipments": "shipment_id",
    }

    for dataset_name, key_column in passthrough_datasets.items():
        metrics.append(
            process_passthrough_dataset(
                spark=spark,
                dataset_name=dataset_name,
                key_column=key_column,
                bronze_dir=bronze_dir,
                silver_dir=silver_dir,
                batch_id=batch_id,
            )
        )

    return metrics


def metrics_to_dataframe(
    spark: SparkSession,
    metrics: list[dict[str, object]],
) -> DataFrame:
    """Convert quality metrics into a Spark DataFrame."""
    dataframe = spark.createDataFrame(metrics)

    return dataframe.withColumn(
        "processed_at",
        F.current_timestamp(),
    )
