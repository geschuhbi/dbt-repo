WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_temp')}}
),
add_weekday AS (
    SELECT *,
        to_char(date, 'Dy') AS weekday, 
        to_char(date, 'DD') AS day_num 
    FROM temp_daily
)
SELECT *
FROM add_weekday