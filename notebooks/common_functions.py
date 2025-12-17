# Databricks notebook source
import uuid
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import datetime, timedelta

# COMMAND ----------

def get_watermark(schema_name: str, table_name: str) -> str | None:
    df = spark.sql(
        f"""
        SELECT watermark_ts
        FROM hen_db.stg.meta_watermark
        WHERE schema_name = '{schema_name}'
          AND table_name = '{table_name}'
        ORDER BY watermark_ts DESC
        LIMIT 1
        """
    )
    row = df.first()
    return row["watermark_ts"] if row else None

# COMMAND ----------

def update_watermark(schema_name: str, table_name: str, watermark_ts):
    if isinstance(watermark_ts, datetime):
        dt = watermark_ts
    else:
        # handle ISO-8601 with Z / milliseconds
        dt = datetime.fromisoformat(watermark_ts.replace("Z", "+00:00"))

    new_watermark_ts = (dt + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    print(f"Updating watermark to {new_watermark_ts}")

    spark.sql(
        f"""
        UPDATE hen_db.stg.meta_watermark
        SET watermark_ts = '{new_watermark_ts}'
        WHERE schema_name = '{schema_name}'
          AND table_name = '{table_name}'
        """
    )

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import uuid

META_LOG_TABLE = "hen_db.stg.meta_log"
VALID_STATUSES = {"RUNNING", "SUCCESS", "FAILED"}

def _normalize_error(error_message: str | None) -> str | None:
    if not error_message:
        return None
    msg = str(error_message).split("\n\n", 1)[0]
    msg = " ".join(msg.splitlines())
    return msg[:500]

def log_run(
    table_name: str,
    run_id: str | None = None,
    row_count: int | None = None,
    run_status: str | None = None,
    error_message: str | None = None,
) -> str | None:

    # START
    if run_id is None:
        run_id = str(uuid.uuid4())

        df = spark.createDataFrame(
            [(run_id, table_name, "RUNNING", None, None, None, None)],
            schema=meta_log_schema,
        ).withColumn("start_time", F.current_timestamp())

        df.write.format("delta").mode("append").saveAsTable(META_LOG_TABLE)
        return run_id

    # END
    if run_status not in VALID_STATUSES:
        raise ValueError(f"Invalid run_status: {run_status}")

    clean_error = _normalize_error(error_message)

    updates = (
        spark.createDataFrame(
            [(run_id, row_count, run_status, clean_error)],
            ["run_id", "row_count", "run_status", "error_message"],
        )
        .withColumn("end_time", F.current_timestamp())
    )

    (
        spark.table(META_LOG_TABLE)
        .alias("t")
        .merge(
            updates.alias("s"),
            "t.run_id = s.run_id"
        )
        .whenMatchedUpdate(set={
            "row_count": "s.row_count",
            "run_status": "s.run_status",
            "end_time": "s.end_time",
            "error_message": "s.error_message",
        })
        .execute()
    )

    return None

