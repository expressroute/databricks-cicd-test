# Databricks notebook source
dbutils.widgets.text("HEVY_API_KEY", "")

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

import requests
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp
from pyspark.sql.functions import col, from_json, explode_outer
import json

# COMMAND ----------

HEVY_API_TOKEN = dbutils.widgets.get("HEVY_API_KEY")
if not HEVY_API_TOKEN:
    raise RuntimeError("HEVY_API_KEY not provided")

BASE_URL = "https://api.hevyapp.com/v1"
HEADERS = {"api-key": HEVY_API_TOKEN, "Accept": "application/json"}

# COMMAND ----------

def hevy_get(endpoint: str, params: dict | None = None) -> dict:
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"Hevy API error {r.status_code}: {r.text}")

    return r.json()

# COMMAND ----------

def get_all_workout_events(since: str, page_size: int = 10) -> list[dict]:
    page = 1
    all_events: list[dict] = []

    while True:
        resp = hevy_get(
            "workouts/events",
            params={"page": page, "pageSize": page_size, "since": since},
        )

        events = resp.get("events", [])
        all_events.extend(events)

        if page >= resp.get("page_count", 0):
            break

        page += 1

    return all_events

# COMMAND ----------

def build_raw_df(events: list[dict]):
    rows_raw = [
        Row(event_type=e.get("type"), json_payload=json.dumps(e, ensure_ascii=False))
        for e in events
    ]

    return spark.createDataFrame(rows_raw).withColumn(
        "ingested_ts", current_timestamp()
    )

# COMMAND ----------

run_id = log_run(table_name="hevy_workout_events")
row_count = 0

try:
    wm = get_watermark("raw", "hevy_workout_events")
    print(f"[RAW] Starting ingestion. Watermark: {wm}")

    events = get_all_workout_events(since=wm)
    print(f"[RAW] Retrieved {len(events)} events")

    if not events:
        print("[RAW] No new events")

        log_run(
            table_name="hevy_workout_events",
            run_id=run_id,
            row_count=0,
            run_status="SUCCESS",
        )

    else:
        df_raw = build_raw_df(events)
        row_count = df_raw.count()

        print(f"[RAW] Writing {row_count} rows to target table")

        (
            df_raw.write.format("delta")
            .mode("append")
            .saveAsTable("hen_db.raw.hevy_workout_events")
        )
        print(f"Writing {row_count} rows to target table")

        # simpler watermark logic: use max of workout.updated_at and workout.deleted_at across all events
        max_ts = None
        for e in events:
            workout = e.get("workout", {})
            ts_list = [workout.get("updated_at"), workout.get("deleted_at")]
            ts_list = [ts for ts in ts_list if ts]
            if ts_list:
                ts = max(ts_list)
                if not max_ts or ts > max_ts:
                    max_ts = ts

        update_watermark(
            schema_name="raw", table_name="hevy_workout_events", watermark_ts=max_ts
        )
        print(f"Watermark updated to: {max_ts}")

        log_run(
            table_name="hevy_workout_events",
            run_id=run_id,
            row_count=row_count,
            run_status="SUCCESS",
        )

    print("Ingestion completed successfully")

except Exception as e:
    print(f"Ingestion failed: {e}")

    log_run(
        table_name="hevy_workout_events",
        run_id=run_id,
        row_count=row_count,
        run_status="FAILED",
        error_message=str(e),
    )
    raise
