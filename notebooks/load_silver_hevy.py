# Databricks notebook source
from pyspark.sql.functions import col

stg_df = spark.table("hen_db.stg.hevy_workout")
print(f"stg table count: {stg_df.count()}")

deleted_workout_ids = stg_df.filter(col("deleted_at").isNotNull()).select("workout_id")
print(f"deleted workouts count: {deleted_workout_ids.count()}")

result_df = stg_df.join(
    deleted_workout_ids,
    on="workout_id",
    how="left_anti"
)
print(f"Cleaned: {result_df.count()}")

# COMMAND ----------

from pyspark.sql.functions import max

latest_workouts = (
    result_df
    .groupBy("workout_id")
    .agg(max("updated_at").alias("latest_updated_at"))
)
print(f"latest_workouts count: {latest_workouts.count()}")

df_latest = (
    result_df
    .join(
        latest_workouts,
        (result_df.workout_id == latest_workouts.workout_id) &
        (result_df.updated_at == latest_workouts.latest_updated_at),
        "inner"
    )
    .drop(latest_workouts.workout_id)
)
print(f"df_latest count: {df_latest.count()}")

# COMMAND ----------

from pyspark.sql.functions import (
    sha2,
    concat_ws,
    col,
    to_timestamp,
    when,
    current_timestamp,
    regexp_replace,
)

cleaned_df = (
    df_latest.select(
        col("workout_id"),
        col("workout_title"),
        to_timestamp(col("start_time")).alias("start_time"),
        to_timestamp(col("end_time")).alias("end_time"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at"),
        col("exercise_index"),
        col("exercise_title"),
        col("set_index"),
        col("weight_kg"),
        col("reps"),
        col("distance_meters"),
        col("duration_seconds"),
        when(col("exercise_title") == "Running", True)
        .otherwise(False)
        .alias("is_running"),
        when(col("exercise_title") != "Running", True)
        .otherwise(False)
        .alias("is_workout"),
    )
    .withColumn(
        "date_key",
        regexp_replace(col("start_time").cast("date").cast("string"), "-", "").cast(
            "int"
        ),
    )
    .withColumn("insert_ts", current_timestamp())
    .withColumn(
        "row_hash",
        sha2(
            concat_ws(
                "||",
                col("workout_id"),
                col("workout_title"),
                col("start_time"),
                col("end_time"),
                col("updated_at"),
                col("exercise_title"),
                col("exercise_index"),
                col("set_index"),
                col("weight_kg"),
                col("reps"),
                col("is_running"),
                col("is_workout"),
            ),
            256,
        ),
    )
)

target_table = "hen_db.stg.silver_workout"
# Create the table if it does not exist
if not spark.catalog.tableExists(target_table):
    cleaned_df.write.saveAsTable(target_table)

# Overwrite the table with new data
cleaned_df.write.mode("overwrite").insertInto(target_table)

print(f"Inserted rows count: {cleaned_df.count()}")
