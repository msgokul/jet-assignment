SELECT DISTINCT views
FROM {{ ref ('fact_xkcd_comics') }}
WHERE views < 0 OR views > 10000