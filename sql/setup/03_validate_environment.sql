-- Validate the Databricks catalog and schema structure.

SHOW CATALOGS;

SHOW SCHEMAS IN dev_databricks_lakehouse;

SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;