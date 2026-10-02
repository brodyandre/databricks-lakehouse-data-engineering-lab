"""Bronze ingestion pipeline for synthetic retail datasets."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from databricks_lakehouse.schemas.customers import CUSTOMERS_SCHEMA
from databricks_lakehouse.schemas.order_items import ORDER_ITEMS_SCHEMA
from databricks_lakehouse.schemas.orders import ORDERS_SCHEMA
from databricks_lakehouse.schemas.payments import PAYMENTS_SCHEMA
from databricks_lakehouse.schemas.products import PRODUCTS_SCHEMA
from databricks_lakehouse.schemas.shipments import SHIPMENTS_SCHEMA

DATASET_SCHEMAS: dict[str, StructType] = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "shipments": SHIPMENTS_SCHEMA,
}


def read_source_csv(
    spark: SparkSession,
    source_path: Path,
    schema: StructType,
    batch_id: str,
) -> DataFrame:
    """Read a CSV source using an explicit schema and add ingestion metadata."""
    dataframe = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(str(source_path))
    )

    return (
        dataframe.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_dataset(
    spark: SparkSession,
    dataset_name: str,
    source_dir: Path,
    bronze_dir: Path,
    batch_id: str,
) -> int:
    """Ingest one dataset into the local Bronze layer."""
    if dataset_name not in DATASET_SCHEMAS:
        raise ValueError(f"Unsupported dataset: {dataset_name}")

    source_path = source_dir / f"{dataset_name}.csv"
    target_path = bronze_dir / dataset_name

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    dataframe = read_source_csv(
        spark=spark,
        source_path=source_path,
        schema=DATASET_SCHEMAS[dataset_name],
        batch_id=batch_id,
    )

    record_count = dataframe.count()

    dataframe.write.mode("overwrite").format("delta").save(str(target_path))

    return record_count


def ingest_all_datasets(
    spark: SparkSession,
    source_dir: Path,
    bronze_dir: Path,
    batch_id: str,
) -> dict[str, int]:
    """Ingest all supported retail datasets into Bronze."""
    counts: dict[str, int] = {}

    for dataset_name in DATASET_SCHEMAS:
        counts[dataset_name] = ingest_dataset(
            spark=spark,
            dataset_name=dataset_name,
            source_dir=source_dir,
            bronze_dir=bronze_dir,
            batch_id=batch_id,
        )

    return counts
