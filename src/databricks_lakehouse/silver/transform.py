"""Silver transformations and data quality rules."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def split_customers_by_quality(dataframe: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split customers into valid and rejected records."""
    invalid_condition = (
        F.col("customer_id").isNull()
        | (F.trim(F.col("customer_id")) == "")
        | F.col("email").isNull()
        | (F.trim(F.col("email")) == "")
    )

    rejected = dataframe.filter(invalid_condition).withColumn(
        "_dq_reason", F.lit("missing_required_customer_fields")
    )

    valid = dataframe.filter(~invalid_condition)

    return valid, rejected


def split_order_items_by_quality(
    dataframe: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split order items into valid and rejected records."""
    invalid_condition = (
        F.col("order_item_id").isNull()
        | (F.trim(F.col("order_item_id")) == "")
        | F.col("order_id").isNull()
        | (F.trim(F.col("order_id")) == "")
        | F.col("product_id").isNull()
        | (F.trim(F.col("product_id")) == "")
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
    )

    rejected = dataframe.filter(invalid_condition).withColumn(
        "_dq_reason", F.lit("invalid_order_item")
    )

    valid = dataframe.filter(~invalid_condition)

    return valid, rejected


def deduplicate_by_key(
    dataframe: DataFrame,
    key_column: str,
) -> DataFrame:
    """Remove duplicate records using a business key."""
    return dataframe.dropDuplicates([key_column])
