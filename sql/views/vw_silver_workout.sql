create or replace view hen_db.stg.vw_silver_workout as
select
  w.workout_id,
  w.workout_title,
  w.exercise_title,
  w.weight_kg,
  w.reps,
  d.date,
  d.day_name,
  d.week_of_year,
  d.month_name,
  d.year
from hen_db.stg.silver_workout w
inner join hen_db.stg.silver_date d on w.date_key = d.date_key
where is_workout = true;
