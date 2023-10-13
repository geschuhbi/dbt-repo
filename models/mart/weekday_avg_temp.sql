with total_avg as (
    select 
        city, country, year, month, week
         AVG(avgtemp_c) AS avg_temp_week,
         MAX(maxtemp_c) AS max_temp_week,
         MIN(mintemp_c) AS min_temp_week
    from {{ref('prep_temp')}}
    group by year, month, country, city
)

select *
from total_avg