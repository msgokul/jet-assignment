WITH source_data AS (
    SELECT * FROM {{ source('xkcd_raw', 'raw_xkcd_comics') }}
)

SELECT
    num AS comic_id, 
    title, 
    safe_title, 
    img_url, 
    alt_text, 
    transcript, 
    year AS published_year, 
    month AS published_month, 
    day AS published_day, 
    MAKE_DATE(year, month, day) AS published_date,
    inserted_at
FROM source_data