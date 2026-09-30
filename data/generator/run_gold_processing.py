"""Run the local PySpark Gold processing pipeline."""

from __future__ import annotations

from pathlib import Path

from databricks_lakehouse.gold.process import process_gold
from databricks_lakehouse.spark import get_spark_session

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")


def main() -> None:
    """Execute Gold processing for all analytical datasets."""
    spark = get_spark_session("retail-gold-processing")

    try:
        counts = process_gold(
            spark=spark,
            silver_dir=SILVER_DIR,
            gold_dir=GOLD_DIR,
        )

        print("Gold processing completed successfully.")

        for dataset_name, record_count in counts.items():
            print(f"{dataset_name}: {record_count:,} rows")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
