create or replace view hen_db.stg.vw_silver_run as
select 
    workout_title,
    exercise_title,
    round(distance_meters / 1000.0, 2) as Distance_KM,
    concat(
        floor(duration_seconds / 60), 
        ':', 
        lpad(cast(mod(duration_seconds, 60) as string), 2, '0')
    ) as Duration_MM_SS,
    format_string(
        '%02d:%02d',
        cast(floor(duration_seconds / nullif(distance_meters / 1000.0, 0) / 60) as int),
        cast(floor(duration_seconds / nullif(distance_meters / 1000.0, 0)) % 60 as int)
    ) as Pace_KM,
    d.date as Date,
    d.month as Month,
    d.week_of_year as Week
from hen_db.stg.silver_workout w
inner join hen_db.stg.silver_date d on w.date_key = d.date_key
where is_running = true;