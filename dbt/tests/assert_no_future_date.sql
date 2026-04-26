SELECT
    comic_id,
    published_date,
    CURRENT_DATE AS today
FROM {{ ref('dim_comics') }}
WHERE published_date > CURRENT_DATE