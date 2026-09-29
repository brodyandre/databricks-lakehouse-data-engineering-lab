"""Schema definition for the orders dataset."""

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_timestamp", TimestampType(), True),
        StructField("order_status", StringType(), True),
    ]
)
