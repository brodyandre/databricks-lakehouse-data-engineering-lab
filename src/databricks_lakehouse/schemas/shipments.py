"""Schema definition for the shipments dataset."""

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

SHIPMENTS_SCHEMA = StructType(
    [
        StructField("shipment_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("shipped_at", TimestampType(), True),
        StructField("delivered_at", TimestampType(), True),
        StructField("delivery_status", StringType(), True),
    ]
)
