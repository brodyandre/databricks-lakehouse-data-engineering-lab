-- Create Medallion and data quality schemas.

CREATE SCHEMA IF NOT EXISTS dev_databricks_lakehouse.bronze;

CREATE SCHEMA IF NOT EXISTS dev_databricks_lakehouse.silver;

CREATE SCHEMA IF NOT EXISTS dev_databricks_lakehouse.gold;

CREATE SCHEMA IF NOT EXISTS dev_databricks_lakehouse.quality;