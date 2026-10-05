# Databricks notebook source
# ruff: noqa: F821

import random
from datetime import UTC, datetime

from faker import Faker
from pyspark.sql.functions import current_timestamp, lit

from databricks_lakehouse.synthetic.generator import (
    DEFAULT_CUSTOMERS,
    DEFAULT_ORDERS,
    DEFAULT_PRODUCTS,
    DEFAULT_SEED,
    generate_customers,
    generate_order_items,
    generate_orders,
    generate_payments,
    generate_products,
    generate_shipments,
    inject_quality_issues,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

if not schema:
    raise ValueError("The schema parameter must not be empty.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

rng = random.Random(DEFAULT_SEED)

faker = Faker("pt_BR")
Faker.seed(DEFAULT_SEED)
faker.seed_instance(DEFAULT_SEED)

customers = generate_customers(faker, rng, DEFAULT_CUSTOMERS)
products = generate_products(faker, rng, DEFAULT_PRODUCTS)
orders = generate_orders(rng, customers, DEFAULT_ORDERS)
order_items = generate_order_items(rng, orders, products)
payments = generate_payments(rng, orders, order_items)
shipments = generate_shipments(rng, orders)

inject_quality_issues(rng, customers, order_items)

datasets = {
    "customers": customers,
    "products": products,
    "orders": orders,
    "order_items": order_items,
    "payments": payments,
    "shipments": shipments,
}

# COMMAND ----------

batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

for dataset_name, rows in datasets.items():
    dataframe = spark.createDataFrame(rows)

    dataframe = dataframe.withColumn(
        "_ingestion_timestamp",
        current_timestamp(),
    ).withColumn(
        "_batch_id",
        lit(batch_id),
    )

    target_table = f"{catalog}.{schema}.bronze_{dataset_name}"

    (
        dataframe.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .saveAsTable(target_table)
    )

    count = spark.table(target_table).count()

    print(f"{target_table}: {count} rows")
