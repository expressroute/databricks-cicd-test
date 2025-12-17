# Databricks notebook source
# MAGIC %run ./common_functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

df_stg = spark.table("hen_db.stg.hevy_workout")
print(f"Staging rows: {df_stg.count()}")
print(f"Distinct workout_id: {df_stg.select('workout_id').distinct().count()}")

w = Window.partitionBy("workout_id")

df_flagged = df_stg.withColumn(
    "has_delete_event",
    F.max(
        (F.col("event_type") == "deleted").cast("int")
    ).over(w)
)

print(f"Rows with delete event present: {df_flagged.filter(F.col('has_delete_event') == 1).count()}")

df_active = df_flagged.filter(F.col("has_delete_event") == 0).drop("has_delete_event")

print(f"Active rows: {df_active.count()}")

# COMMAND ----------

target_table = "silver_workout"
tz = "Europe/Copenhagen"
run_id = log_run(table_name=target_table)
row_count = 0

try:
    df_selected = (
        df_active
        .withColumn("local_start_time", F.from_utc_timestamp(F.to_timestamp("start_time"), tz))
        .withColumn("local_end_time", F.from_utc_timestamp(F.to_timestamp("end_time"), tz))
        .withColumn("local_created_at", F.from_utc_timestamp(F.to_timestamp("created_at"), tz))
        .withColumn("local_updated_at", F.from_utc_timestamp(F.to_timestamp("updated_at"), tz))
        .select(
            "workout_id",
            "workout_title",
            "exercise_index",
            "exercise_title",
            "set_index",
            "weight_kg",
            "reps",
            "distance_meters",
            "duration_seconds",
            "local_start_time",
            "local_end_time",
            "local_created_at",
            "local_updated_at",
            (F.col("exercise_title") == "Running").alias("is_running"),
            (F.col("exercise_title") != "Running").alias("is_workout"),
            F.date_format("local_start_time", "yyyyMMdd").cast("int").alias("date_key"),
            F.concat(F.lpad(F.hour("local_start_time"), 2, "0"), F.lit("00")).cast("int").alias("time_key"),
            F.from_utc_timestamp(F.current_timestamp(), tz).alias("insert_ts"),
        )
    )
    
    (
        df_selected
        .write
        .mode("overwrite")
        .saveAsTable(f"hen_db.stg.{target_table}")
    )

    row_count = df_selected.count()

    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="SUCCESS",
    )

    print("[Silver] completed successfully")
    print(f"[Silver] Rows inserted: {row_count}")

except Exception as e:
    print(f"[Silver] Ingestion FAILED: {e}")
    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="FAILED",
        error_message=str(e),
    )
    raise
