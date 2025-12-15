# Databricks notebook source
from pyspark.sql.functions import col

stg_df = spark.table("hen_db.stg.hevy_workout")

deleted_workout_ids = stg_df.filter(col("deleted_at").isNotNull()).select("workout_id")

result_df = df.join(
    deleted_workout_ids,
    on="workout_id",
    how="left_anti"
)

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import when

clean_df = (
    result_df
    .select(
        col("workout_id"),
        col("workout_title"),
        col("start_time"),
        col("end_time"),
        col("created_at"),
        col("exercise_index"),
        col("exercise_title"),
        col("distance_meters"),
        col("duration_seconds"),
        when((col("exercise_index") == 0) & (col("exercise_title") == "Running"), True).otherwise(False).alias("is_running"),
        when((col("exercise_index") == 0) & (col("exercise_title") == "Running"), True).otherwise(False).alias("is_running")
    )
)
display(clean_df)
