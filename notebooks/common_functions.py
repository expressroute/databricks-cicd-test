# Databricks notebook source
import uuid
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import datetime, timedelta
from datetime import datetime

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

meta_log_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("run_status", StringType(), False),
        StructField("row_count", IntegerType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ]
)


def log_run_OLD(
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
        ).withColumn("start_time", current_timestamp())

        (df.write.format("delta").mode("append").saveAsTable("hen_db.stg.meta_log"))

        return run_id

    # END
    else:
        # normalize error message inline
        clean_error = None
        if error_message:
            msg = str(error_message)
            msg = msg.split("\n\n", 1)[0]  # keep first paragraph
            msg = " ".join(msg.splitlines())  # force single line
            clean_error = msg[:500]  # hard limit

            # escape quotes for SQL
            clean_error = clean_error.replace("'", "''")

        spark.sql(
            f"""
            UPDATE hen_db.stg.meta_log
            SET
                row_count = {row_count},
                run_status = '{run_status}',
                end_time = current_timestamp(),
                error_message = {f"'{clean_error}'" if clean_error else "NULL"}
            WHERE run_id = '{run_id}'
        """
        )

        return None

# COMMAND ----------

meta_log_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("run_status", StringType(), False),
        StructField("row_count", IntegerType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ]
)

def log_run(
    table_name: str,
    run_id: str = None,
    row_count: int = None,
    run_status: str = "RUNNING",
    error_message: str = None,
):
    if run_id is None:
        run_id = str(uuid.uuid4())
        df = spark.createDataFrame(
            [
                (
                    run_id,
                    table_name,
                    run_status,
                    row_count,
                    None,
                    None,
                    error_message,
                )
            ],
            schema=meta_log_schema,
        ).withColumn("start_time", current_timestamp())
        df.write.format("delta").mode("append").saveAsTable("hen_db.stg.meta_log")
        return run_id
    else:
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        set_clause = (
            f"row_count = {row_count if row_count is not None else 'NULL'}, "
            f"run_status = '{run_status}', "
            f"end_time = '{end_time}', "
            f"error_message = '{error_message}'"
        )
        spark.sql(
            f"""
            UPDATE hen_db.stg.meta_log
            SET {set_clause}
            WHERE run_id = '{run_id}'
            """
        )
        return None
