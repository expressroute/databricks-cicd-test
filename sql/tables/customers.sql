USE CATALOG IDENTIFIER(:catalog_param);
USE SCHEMA IDENTIFIER(:schema_param);

CREATE TABLE customers (
    customer_id INT,
    name STRING,
    created_at TIMESTAMP
)
USING DELTA;