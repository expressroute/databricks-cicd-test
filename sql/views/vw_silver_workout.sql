create or replace view hen_db.gold.vw_workout as
select
  w.workout_id,
  w.workout_title,
  w.local_start_time as StartTime,
  w.local_end_time as EndTime,
  ceil((unix_timestamp(w.local_end_time) - unix_timestamp(w.local_start_time)) / 60) as duration_minutes,
  d.date,
  d.day_name,
  d.week_of_year,
  d.month_name,
  d.year,
  t.hour_24
from hen_db.silver.workout w
inner join hen_db.silver.dim_date d on w.date_key = d.date_key
inner join hen_db.silver.dim_time t on w.time_key = t.time_key
where is_workout = true


/*
CREATE CATALOG IF NOT EXISTS hen_db;

CREATE SCHEMA IF NOT EXISTS hen_db.meta;
CREATE SCHEMA IF NOT EXISTS hen_db.raw;
CREATE SCHEMA IF NOT EXISTS hen_db.stg;
CREATE SCHEMA IF NOT EXISTS hen_db.silver;
CREATE SCHEMA IF NOT EXISTS hen_db.gold;

*/