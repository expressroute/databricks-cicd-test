# Databricks notebook source
# MAGIC %run ./common_functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

source_table ="hevy_workout"
target_table = "workout"

# COMMAND ----------

df_base = spark.table(f"hen_db.stg.{source_table}")
print(
    f"Distinct workout_ids: {df_base.select('workout_id').distinct().count()}"
)

# find workout_ids that have been deleted
df_deleted_ids = (
    df_base.filter(F.col("deleted_at").isNotNull()).select("workout_id").distinct()
)
print(f"Deleted workout_ids: {df_deleted_ids.count()}")

w_latest = Window.partitionBy("workout_id").orderBy(F.col("updated_at").desc())

df_selected = (
    df_base.join(df_deleted_ids, on="workout_id", how="left_anti")
    .select(
        "event_type",
        "workout_id",
        "workout_title",
        "start_time",
        "end_time",
        "updated_at",
        "created_at",
        "deleted_at",
        F.when(F.col("exercise_title") == "Running", F.col("distance_meters")).otherwise(0).alias("distance_meters"),
        F.when(F.col("exercise_title") == "Running", F.col("duration_seconds")).otherwise(0).alias("duration_seconds"),
    )
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .dropDuplicates()
)

print(f"Selected rows: {df_selected.count()}")

# COMMAND ----------

tz = "Europe/Copenhagen"
batch_ts = datetime.now()
run_id = log_run(table_name=target_table)
row_count = 0

try:
    df_stg = (
        df_selected
        .withColumn("local_start_time", F.from_utc_timestamp(F.to_timestamp("start_time"), tz))
        .withColumn("local_end_time", F.from_utc_timestamp(F.to_timestamp("end_time"), tz))
        .withColumn("local_created_at", F.from_utc_timestamp(F.to_timestamp("created_at"), tz))
        .withColumn("local_updated_at", F.from_utc_timestamp(F.to_timestamp("updated_at"), tz))
        .select(
            "workout_id",
            "workout_title",
            "distance_meters",
            "duration_seconds",
            "local_start_time",
            "local_end_time",
            "local_created_at",
            "local_updated_at",
            (F.col("workout_title") == "Run").alias("is_running"),
            (F.col("workout_title") != "Run").alias("is_workout"),
            F.date_format("local_start_time", "yyyyMMdd").cast("int").alias("date_key"),
            F.format_string("%02d%02d",F.hour("local_start_time"),F.minute("local_start_time")).cast("int").alias("time_key"),
            F.from_utc_timestamp(F.lit(batch_ts), tz).alias("insert_ts"),
        )
    )
    
    (
        df_stg
        .write
        .mode("overwrite")
        .saveAsTable(f"hen_db.silver.{target_table}")
    )

    row_count = df_stg.count()

    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="SUCCESS",
    )

    print(f"Completed. Rows inserted: {row_count}")

except Exception as e:
    print(f"Ingestion FAILED: {e}")
    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="FAILED",
        error_message=str(e),
    )
    raise
