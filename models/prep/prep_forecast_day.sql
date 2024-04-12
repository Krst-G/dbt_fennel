WITH forecast_day_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_day')}}
),
add_features AS (
    SELECT *,
        DATE_PART('day', date) AS day_of_month,
        TO_CHAR(date, 'Month') AS month_of_year,
        DATE_PART('year', date) AS year,
        TO_CHAR(date, 'Day') AS day_of_week,
        DATE_PART('week', date) AS week_of_year,
        TO_CHAR(date, 'YYYY-IW') AS year_and_week -- Year and ISO week
    FROM forecast_day_data
)
SELECT *
FROM add_features