# Databricks notebook source
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
