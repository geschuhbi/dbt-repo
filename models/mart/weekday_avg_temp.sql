WITH weekly_avg AS (
    SELECT 
        city, country, year, week,
        AVG(avgtemp_c) AS avg_temp_week,
        MAX(maxtemp_c) AS max_temp_week,
        MIN(mintemp_c) AS min_temp_week
    FROM {{ref('prep_temp')}}
    GROUP BY city, country, year, week
),
monthly_avg AS (
    SELECT 
        city, country, year, month,
        AVG(avgtemp_c) AS avg_temp_month,
        MAX(maxtemp_c) AS max_temp_month,
        MIN(mintemp_c) AS min_temp_month
    FROM {{ref('prep_temp')}}
    GROUP BY city, country, year, month
)
SELECT 
    city, country, year, weekday,
    AVG(avgtemp_c) AS avg_temp_weekday,
    MAX(maxtemp_c) AS max_temp_weekday,
    MIN(mintemp_c) AS min_temp_weekday
FROM {{ref('prep_temp')}}
GROUP BY city, country, year, weekday
UNION ALL
SELECT 
    city, country, year, week, NULL AS weekday,
    avg_temp_week, max_temp_week, min_temp_week
FROM weekly_avg
UNION ALL
SELECT 
    city, country, year, month, NULL AS weekday,
    avg_temp_month, max_temp_month, min_temp_month
FROM monthly_avg;