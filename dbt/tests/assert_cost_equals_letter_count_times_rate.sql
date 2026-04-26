SELECT
    f.comic_id,
    f.cost_euros AS actual_cost,
    dc.title_letter_count * 5 AS expected_cost
FROM {{ ref('fact_xkcd_comics') }} f
JOIN {{ ref('dim_comics') }} dc USING (comic_id)
WHERE f.cost_euros != dc.title_letter_count * 5