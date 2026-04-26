WITH date_details AS (
    SELECT DISTINCT published_date
    FROM {{ ref('stg_xkcd_comics') }}
)

SELECT
    TO_CHAR(published_date, 'YYYYMMDD') AS date_key,
    published_date,
    EXTRACT(YEAR FROM published_date) AS published_year,
    EXTRACT(MONTH FROM published_date) AS published_month,
    EXTRACT(DAY FROM published_date) AS published_day,
    EXTRACT(DOW FROM published_date) AS day_of_week,
    'Q' || EXTRACT(QUARTER FROM published_date)::CHAR AS quarter,
    TRIM(TO_CHAR(published_date, 'Day')) as day_name, 
    EXTRACT(DOY FROM published_date) AS day_of_year,
    CASE 
        WHEN EXTRACT(DOW FROM published_date) IN (0, 6) 
        THEN TRUE ELSE FALSE 
    END AS is_weekend
FROM date_details
ORDER BY published_date