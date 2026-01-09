# Databricks notebook source
from pyspark.sql import functions as F

table_name = "hen_db.silver.dim_time"

dim_time = (
    spark.range(0, 24 * 60)
    .withColumnRenamed("id", "minute_of_day")
    .withColumn("hour_24", (F.col("minute_of_day") / 60).cast("int"))
    .withColumn("minute", (F.col("minute_of_day") % 60).cast("int"))
    .withColumn(
        "time_key",
        F.format_string("%02d%02d", F.col("hour_24"), F.col("minute")).cast("int")
    )
    .withColumn("time_label", F.format_string("%02d:%02d", F.col("hour_24"), F.col("minute")))
    .select("time_key", "hour_24", "minute", "time_label")
)

(
    dim_time
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(table_name)
)
