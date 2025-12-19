create or replace view hen_db.stg.vw_silver_workout as
select
  w.workout_id,
  w.workout_title,
  w.exercise_title,
  w.weight_kg,
  w.reps,
  w.local_start_time as StartTime,
  w.local_end_time as EndTime,
  ceil((unix_timestamp(w.local_end_time) - unix_timestamp(w.local_start_time)) / 60) as duration_minutes,
  d.date,
  d.day_name,
  d.week_of_year,
  d.month_name,
  d.year,
  t.hour_24,
  t.hour_label
from hen_db.stg.silver_workout w
inner join hen_db.stg.silver_date d on w.date_key = d.date_key
inner join hen_db.stg.silver_dim_time t on w.time_key = t.time_key
where is_workout = true;