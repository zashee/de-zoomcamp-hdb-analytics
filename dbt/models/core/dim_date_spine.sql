WITH date_spine AS (

{{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2017-01-01' as date)",
        end_date="DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)"
       )
    }}

)


SELECT
    DATE(date_day) as date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month_number,
    FORMAT_DATE('%B', date_day) AS month_name,
FROM date_spine