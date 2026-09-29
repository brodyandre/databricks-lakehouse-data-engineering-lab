"""Schema definition for the payments dataset."""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

PAYMENTS_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("payment_method", StringType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("payment_timestamp", TimestampType(), True),
    ]
)
