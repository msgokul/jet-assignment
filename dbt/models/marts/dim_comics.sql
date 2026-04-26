WITH stg AS (
    SELECT * from {{ ref('stg_xkcd_comics') }}
)

SELECT 
    comic_id, 
    title, 
    safe_title,
    img_url,
    alt_text,
    transcript, 
    published_date, 
    LENGTH(REGEXP_REPLACE(title, '[^a-zA-Z]','','g')) AS title_letter_count,
    LENGTH(title) AS title_char_count,
    inserted_at
FROM stg

