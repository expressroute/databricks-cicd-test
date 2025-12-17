# Databricks notebook source
# DBTITLE 1,he e
from pyspark.sql import functions as F

# ---- Parameters ----
start_date = "2024-01-01"
end_date   = "2035-12-31"
table_name = "hen_db.stg.silver_date"

# ---- Generate date dimension (always) ----
df_date = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS date
    """)
)

dim_date = (
    df_date
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))   # 1=Sun … 7=Sat
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
)


# ---- Table existence check + write ----
if not spark.catalog.tableExists(table_name):
    (
        dim_date
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(table_name)
    )
else:
    print(f"Table '{table_name}' already exists.")

# COMMAND ----------

from pyspark.sql import functions as F

# ---- Parameters ----
table_name = "hen_db.stg.silver_dim_time"

# ---- Generate time dimension (always) ----
dim_time = (
    spark.range(0, 24)
    .withColumnRenamed("id", "hour")
    .withColumn("time_key", (F.col("hour") * 100).cast("int"))   # 0, 100, ..., 2300
    .withColumn("hour_24", F.col("hour"))
    .withColumn("hour_label", F.format_string('%02d:00', F.col("hour")))
    .select(
        "time_key",
        "hour_24",
        "hour_label"
    )
)

# ---- Table existence check + write ----
if not spark.catalog.tableExists(table_name):
    (
        dim_time
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(table_name)
    )
else:
    print(f"Table '{table_name}' already exists.")

