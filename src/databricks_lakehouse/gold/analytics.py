"""Gold analytical transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_ORDER_STATUSES = ("completed", "shipped")


def build_eligible_orders(
    orders: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """Return commercial orders associated with valid Silver customers."""
    valid_customers = customers.select("customer_id").distinct()

    return orders.filter(F.col("order_status").isin(*VALID_ORDER_STATUSES)).join(
        valid_customers,
        on="customer_id",
        how="inner",
    )


def build_order_revenue(
    orders: DataFrame,
    order_items: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """Build revenue metrics at order level."""
    eligible_orders = build_eligible_orders(
        orders=orders,
        customers=customers,
    )

    order_totals = order_items.groupBy("order_id").agg(
        F.sum("line_amount").alias("order_revenue"),
        F.sum("quantity").alias("items_sold"),
        F.countDistinct("product_id").alias("distinct_products"),
    )

    return eligible_orders.join(order_totals, on="order_id", how="inner").select(
        "order_id",
        "customer_id",
        "order_timestamp",
        "order_status",
        "order_revenue",
        "items_sold",
        "distinct_products",
    )


def build_product_revenue(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """Build revenue metrics at product level."""
    eligible_orders = build_eligible_orders(
        orders=orders,
        customers=customers,
    )

    valid_items = order_items.join(
        eligible_orders.select("order_id"),
        on="order_id",
        how="inner",
    )

    return (
        valid_items.join(products, on="product_id", how="inner")
        .groupBy(
            "product_id",
            "product_name",
            "category",
        )
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("line_amount").alias("product_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )


def build_category_revenue(
    product_revenue: DataFrame,
) -> DataFrame:
    """Build revenue metrics at product category level."""
    return product_revenue.groupBy("category").agg(
        F.sum("units_sold").alias("units_sold"),
        F.sum("product_revenue").alias("category_revenue"),
        F.sum("order_count").alias("product_order_occurrences"),
    )


def build_customer_revenue(
    customers: DataFrame,
    order_revenue: DataFrame,
) -> DataFrame:
    """Build customer-level commercial metrics."""
    customer_metrics = order_revenue.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("order_revenue").alias("customer_revenue"),
        F.avg("order_revenue").alias("average_order_value"),
        F.sum("items_sold").alias("items_purchased"),
    )

    return customers.join(customer_metrics, on="customer_id", how="inner").select(
        "customer_id",
        "full_name",
        "email",
        "state",
        "order_count",
        "customer_revenue",
        "average_order_value",
        "items_purchased",
    )


def build_business_kpis(
    order_revenue: DataFrame,
) -> DataFrame:
    """Build high-level commercial KPIs."""
    return order_revenue.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("active_customers"),
        F.sum("order_revenue").alias("total_revenue"),
        F.avg("order_revenue").alias("average_order_value"),
        F.sum("items_sold").alias("total_items_sold"),
    )
