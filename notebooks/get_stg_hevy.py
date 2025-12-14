# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)
from pyspark.sql.functions import col, from_json, explode_outer

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

source_table = "hevy_workout_events"
target_table = "hevy_workout"

# COMMAND ----------

set_schema = StructType(
    [
        StructField("index", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("weight_kg", DoubleType(), True),
        StructField("reps", IntegerType(), True),
        StructField("distance_meters", IntegerType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("rpe", IntegerType(), True),
        StructField("custom_metric", StringType(), True),
    ]
)

exercise_schema = StructType(
    [
        StructField("index", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("exercise_template_id", StringType(), True),
        StructField("superset_id", StringType(), True),
        StructField("sets", ArrayType(set_schema), True),
    ]
)

workout_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("routine_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("exercises", ArrayType(exercise_schema), True),
    ]
)

# COMMAND ----------

df_raw = spark.table(f"hen_db.raw.{source_table}")

event_payload_schema = StructType(
    [
        StructField("type", StringType(), True),
        StructField("workout", workout_schema, True),
    ]
)

df_parsed = df_raw.withColumn(
    "event", from_json(col("json_payload"), event_payload_schema)
).select(col("event.type").alias("event_type"), 
         col("event.workout").alias("workout"))

# COMMAND ----------

stg_wm = get_watermark("stg", target_table)
print(f"[STG] Starting ingestion. Watermark: {stg_wm}")

df_events = df_parsed

if stg_wm:
    df_events = df_events.filter(col("workout.updated_at") > stg_wm)

# COMMAND ----------

df_stg_full = (
    df_events
    .withColumn("exercise", explode_outer(col("workout.exercises")))
    .withColumn("set", explode_outer(col("exercise.sets")))
    .select(
        col("event_type"),
        col("workout.id").alias("workout_id"),
        col("workout.title").alias("workout_title"),
        col("workout.routine_id"),
        col("workout.start_time"),
        col("workout.end_time"),
        col("workout.updated_at"),
        col("workout.created_at"),
        col("exercise.index").alias("exercise_index"),
        col("exercise.title").alias("exercise_title"),
        col("exercise.exercise_template_id"),
        col("exercise.superset_id"),
        col("set.index").alias("set_index"),
        col("set.type").alias("set_type"),
        col("set.weight_kg"),
        col("set.reps"),
        col("set.distance_meters"),
        col("set.duration_seconds"),
        col("set.rpe"),
        col("set.custom_metric"),
    )
)

# COMMAND ----------

run_id = log_run(table_name=target_table)
row_count = 0

try:
    if not df_stg_full.head(1):
        print("[STG] No new records to process")

        log_run(
            table_name=target_table,
            run_id=run_id,
            row_count=0,
            run_status="SUCCESS",
        )

    else:
        row_count = df_stg_full.count()
        print(f"[STG] Writing {row_count} rows to table")

        (
            df_stg_full.write.format("delta")
            .mode("append")
            .saveAsTable(f"hen_db.stg.{target_table}")
        )

        # derive new watermark from data
        max_ts = df_stg_full.selectExpr("max(updated_at) as max_ts").collect()[0]["max_ts"]

        update_watermark(
            schema_name="stg", table_name=target_table, watermark_ts=max_ts
        )

        log_run(
            table_name=target_table,
            run_id=run_id,
            row_count=row_count,
            run_status="SUCCESS",
        )

    print("[STG] Ingestion completed successfully")

except Exception as e:
    print(f"[STG] Ingestion FAILED: {e}")

    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="FAILED",
        error_message=str(e),
    )
    raise
