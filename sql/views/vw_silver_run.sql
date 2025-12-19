create or replace view hen_db.stg.vw_silver_run as
select 
    workout_title,
    exercise_title,
    distance_meters,
    duration_seconds,
    -- Pace_KM as float value (minutes.seconds per km)
    cast(floor(duration_seconds / nullif(distance_meters / 1000.0, 0) / 60) as double) +
    cast(floor(duration_seconds / nullif(distance_meters / 1000.0, 0)) % 60 as double) / 100.0 as Pace_KM_MinSecFloat,
    d.date as Date,
    d.month as Month
from hen_db.stg.silver_workout w
inner join hen_db.stg.silver_date d on w.date_key = d.date_key
where is_running = true
group by 
    workout_title,
    exercise_title,
    distance_meters,
    duration_seconds,
    d.date,
    d.month;