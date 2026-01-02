CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.reviews_agg` AS 
SELECT
    order_id,
    ARRAY_AGG(score ORDER BY creation_date DESC LIMIT 1)[OFFSET(0)] AS score
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.reviews` 
WHERE score IS NOT NULL
GROUP BY order_id;
