# Databricks notebook source
dbutils.widgets.text("HEVY_API_KEY", "")

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

import requests
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, current_timestamp, from_utc_timestamp
from pyspark.sql.functions import col, from_json, explode_outer
import json

# COMMAND ----------

HEVY_API_TOKEN = dbutils.widgets.get("HEVY_API_KEY")
if not HEVY_API_TOKEN:
    raise RuntimeError("HEVY_API_KEY not provided")
BASE_URL = "https://api.hevyapp.com/v1"
HEADERS = {"api-key": HEVY_API_TOKEN, "Accept": "application/json"}

# COMMAND ----------

target_table = "hevy_workout_events"

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

run_id = log_run(table_name=target_table)

wm = get_watermark("raw", target_table)
print(f"Starting ingestion. Watermark: {wm}")

events = get_all_workout_events(since=wm)
print(f"Retrieved {len(events)} events")

if not events:
    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="SUCCESS",
    )
    dbutils.notebook.exit("No new events")

# COMMAND ----------

try:
    df_raw = spark.createDataFrame(
        [
            Row(
                event_type=e.get("type"),
                json_payload=json.dumps(e, ensure_ascii=False),
            )
            for e in events
        ]
    ).withColumn(
        "ingested_ts",
        from_utc_timestamp(current_timestamp(), "Europe/Copenhagen"),
    )

    row_count = df_raw.count()
    print(f"Writing {row_count} rows to target table")

    (
        df_raw.write.format("delta")
        .mode("append")
        .saveAsTable(f"hen_db.raw.{target_table}")
    )

    # Find the highest timestamp from updated_at and deleted_at in all events
    max_ts = max(
        [
            ts
            for e in events
            for ts in [e.get("workout", {}).get("updated_at"), e.get("deleted_at")]
            if ts
        ],
        default=None,
    )

    # Update watermark
    update_watermark(schema_name="raw", table_name=target_table, watermark_ts=max_ts)

    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="SUCCESS",
    )

    print("Ingestion completed successfully")

except Exception as e:
    print(f"Ingestion failed: {e}")

    log_run(
        table_name=target_table,
        run_id=run_id,
        row_count=row_count,
        run_status="FAILED",
        error_message=str(e),
    )
    raise
