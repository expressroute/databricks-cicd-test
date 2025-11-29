CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.customers (
    customer_id INT,
    name STRING,
    created_at TIMESTAMP
)
USING DELTA;