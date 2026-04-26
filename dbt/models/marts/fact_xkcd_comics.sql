WITH comic_dim AS (
        SELECT 
            comic_id, 
            title_letter_count,
            published_date
        FROM {{ ref('dim_comics') }}
), 

    date_dim AS (
        SELECT 
            date_key,
            published_date
        FROM {{ ref('dim_date') }}
), 

    metrics AS (
        SELECT 
            cd.comic_id, 
            dd.date_key,
            cd.title_letter_count * 5 AS cost_euros,
            ROUND((RANDOM() * 10000)::numeric, 0) AS views,
            ROUND((1.0 + RANDOM() * 9.0)::numeric, 1) AS reviews
        FROM comic_dim cd
        JOIN date_dim dd ON cd.published_date = dd.published_date
    )

SELECT 
    CONCAT(comic_id,'-', date_key) AS fact_id,
    comic_id, 
    date_key, 
    cost_euros, 
    views, 
    reviews
FROM metrics

