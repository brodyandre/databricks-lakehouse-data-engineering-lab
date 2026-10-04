# Databricks notebook source
# ruff: noqa: F821

from pyspark.sql import functions as F

# COMMAND ----------

current_user = spark.sql("SELECT current_user() AS user").first()["user"]

print(f"Authenticated Databricks user: {current_user}")

# COMMAND ----------

dataframe = spark.range(1, 6).withColumn(
    "processed_at",
    F.current_timestamp(),
)

display(dataframe)

# COMMAND ----------

assert dataframe.count() == 5

print("Databricks bundle smoke test completed successfully.")
