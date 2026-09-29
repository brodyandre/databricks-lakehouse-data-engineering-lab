"""Schema definition for the order_items dataset."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("line_amount", DoubleType(), True),
    ]
)
