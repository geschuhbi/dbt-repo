WITH month_avg AS (
    SELECT
        city, country,year, month, month_num, lat, lon,
        AVG(avgtemp_c) AS avg_temp_monthly,
        MAX(maxtemp_c) AS max_temp_monthly,
        MIN(mintemp_c) AS min_temp_monthly,
        AVG(avghumidity) AS avg_humidity_monthly
    FROM {{ref('prep_temp')}}
    GROUP BY city, country, year, month, month_num, lat, lon
)
SELECT *
FROM month_avg