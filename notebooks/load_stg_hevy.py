# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, explode_outer, greatest, coalesce

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

event_payload_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),              # for deleted events
    StructField("deleted_at", StringType(), True),      # for deleted events
    StructField("workout", workout_schema, True),       # for updated events
])


# COMMAND ----------

df_raw = spark.table(f"hen_db.raw.{source_table}")

df_parsed = (
    df_raw
    .withColumn("event", from_json(col("json_payload"), event_payload_schema))
    .select(
        col("event.type").alias("event_type"),
        col("event.id").alias("event_id"),
        col("event.deleted_at").alias("event_deleted_at"),
        col("event.workout").alias("workout"),
    )
)

# COMMAND ----------

stg_wm = get_watermark("stg", target_table)
print(f"[STG] Starting ingestion. Latest watermark: {stg_wm}")

df_events = df_parsed

if stg_wm:
    df_events = df_events.filter(
        greatest(
            col("workout.updated_at"),
            col("event_deleted_at")
        ) > stg_wm
    )

# COMMAND ----------

df_stg_full = (
    df_events.withColumn("exercise", explode_outer(col("workout.exercises")))
    .withColumn("set", explode_outer(col("exercise.sets")))
    .select(
        col("event_type"),
        # unified workout id (updated OR deleted)
        coalesce(col("workout.id"), col("event_id")).alias("workout_id"),
        # workout attributes (NULL for deletes)
        col("workout.title").alias("workout_title"),
        col("workout.routine_id"),
        col("workout.start_time"),
        col("workout.end_time"),
        col("workout.updated_at").alias("updated_at"),
        col("workout.created_at").alias("created_at"),
        # only for deleted events
        col("event_deleted_at").alias("deleted_at"),
        # exercise & sets (NULL for deletes)
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

        # derive new watermark from data: highest of updated_at or deleted_at
        max_ts = (
            df_stg_full
            .selectExpr("max(greatest(updated_at, deleted_at)) as max_ts")
            .collect()[0]["max_ts"]
        )

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
