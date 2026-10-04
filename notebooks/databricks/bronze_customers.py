# Databricks notebook source
# ruff: noqa: F821

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

if not schema:
    raise ValueError("The schema parameter must not be empty.")

target_table = f"{catalog}.{schema}.bronze_customers"

print(f"Target table: {target_table}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

customer_schema = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("full_name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("state", StringType(), False),
    ]
)

customers = [
    (1, "Ana Silva", "ana.silva@example.com", "SP"),
    (2, "Carlos Souza", "carlos.souza@example.com", "RJ"),
    (3, "Mariana Oliveira", "mariana.oliveira@example.com", "MG"),
    (4, "Joao Santos", None, "SP"),
    (5, "Fernanda Lima", "fernanda.lima@example.com", "PR"),
]

dataframe = spark.createDataFrame(
    customers,
    schema=customer_schema,
).withColumn(
    "_ingestion_timestamp",
    F.current_timestamp(),
)

# COMMAND ----------

(dataframe.write.mode("overwrite").format("delta").saveAsTable(target_table))

# COMMAND ----------

result = spark.table(target_table)

print(f"Bronze customer rows: {result.count()}")

display(result.orderBy("customer_id"))

assert result.count() == 5

print("Bronze customers table created successfully.")
