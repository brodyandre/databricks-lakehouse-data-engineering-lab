"""Run the local PySpark Bronze ingestion pipeline."""

from __future__ import annotations

from pathlib import Path

from databricks_lakehouse.bronze.ingest import ingest_all_datasets
from databricks_lakehouse.spark import get_spark_session

SOURCE_DIR = Path("data/sample")
BRONZE_DIR = Path("data/bronze")
BATCH_ID = "local-bootstrap-001"


def main() -> None:
    """Execute Bronze ingestion for all retail datasets."""
    spark = get_spark_session("retail-bronze-ingestion")

    try:
        counts = ingest_all_datasets(
            spark=spark,
            source_dir=SOURCE_DIR,
            bronze_dir=BRONZE_DIR,
            batch_id=BATCH_ID,
        )

        print("Bronze ingestion completed successfully.")

        for dataset_name, record_count in counts.items():
            print(f"{dataset_name}: {record_count:,} rows")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
