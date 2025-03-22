WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2017-01-01' as date)",
        end_date="date_add(current_date(), interval 1 year)"
    )
    }}
)

SELECT
    date_month AS date_key,
    EXTRACT(YEAR FROM date_month) AS year,
    EXTRACT(MONTH FROM date_month) AS month_number,
    FORMAT_DATE('%B', date_month) AS month_name,
    FORMAT_DATE('%Y-%m', date_month) AS year_month,
    CONCAT('Q', CAST(CEIL(EXTRACT(MONTH FROM date_month) / 3) AS STRING)) AS quarter,
    CONCAT(CAST(EXTRACT(YEAR FROM date_month) AS STRING), ' ',
        'Q', CAST(CEIL(EXTRACT(MONTH FROM date_month) / 3) AS STRING)
    ) AS year_quarter
FROM date_spine