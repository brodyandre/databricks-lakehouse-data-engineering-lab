"""Schema definition for the customers dataset."""

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("state", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)
