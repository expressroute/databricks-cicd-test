CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.customers (
    customer_id BIGINT GENERATED ALWAYS IDENTITY,
    name STRING,
    created_at TIMESTAMP
)
USING DELTA;