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
    t.city, t.country, t.year, t.weekday,
    AVG(t.avgtemp_c) AS avg_temp_weekday,
    MAX(t.maxtemp_c) AS max_temp_weekday,
    MIN(t.mintemp_c) AS min_temp_weekday,
    w.avg_temp_week, w.max_temp_week, w.min_temp_week,
    m.avg_temp_month, m.max_temp_month, m.min_temp_month
FROM {{ref('prep_temp')}} AS t
LEFT JOIN weekly_avg AS w USING (city, country, year, week)
LEFT JOIN monthly_avg AS m USING (city, country, year, month)
GROUP BY t.city, t.country, t.year, t.weekday