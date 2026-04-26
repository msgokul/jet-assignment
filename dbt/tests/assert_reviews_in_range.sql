SELECT DISTINCT reviews
FROM {{ ref ('fact_xkcd_comics') }}
WHERE reviews < 1.0 OR reviews > 10
