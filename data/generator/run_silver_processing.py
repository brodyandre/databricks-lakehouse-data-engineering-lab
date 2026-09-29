"""Run the local PySpark Silver processing pipeline."""

from __future__ import annotations

from pathlib import Path

from databricks_lakehouse.silver.process import (
    metrics_to_dataframe,
    process_all_datasets,
)
from databricks_lakehouse.spark import get_spark_session

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")
REJECTED_DIR = Path("data/rejected")
QUALITY_DIR = Path("data/quality")
BATCH_ID = "local-silver-001"


def main() -> None:
    """Execute Silver processing for all retail datasets."""
    spark = get_spark_session("retail-silver-processing")

    try:
        metrics = process_all_datasets(
            spark=spark,
            bronze_dir=BRONZE_DIR,
            silver_dir=SILVER_DIR,
            rejected_dir=REJECTED_DIR,
            batch_id=BATCH_ID,
        )

        metrics_df = metrics_to_dataframe(
            spark=spark,
            metrics=metrics,
        )

        (metrics_df.write.mode("overwrite").format("parquet").save(str(QUALITY_DIR)))

        print("Silver processing completed successfully.")

        for metric in metrics:
            print(
                f"{metric['dataset']}: "
                f"input={metric['input_records']:,} "
                f"valid={metric['valid_records']:,} "
                f"rejected={metric['rejected_records']:,} "
                f"quality_rate={metric['quality_rate']:.4f}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
