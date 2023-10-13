WITH temperature_daily AS (
    SELECT * 
    FROM {{ref('staging_temp')}}
),
add_weekday AS (
    SELECT *,
        to_char(date, 'Day') AS weekday, 
        to_char(date, 'DD') AS day_num 
    FROM temperature_daily
)
SELECT *
FROM add_weekday