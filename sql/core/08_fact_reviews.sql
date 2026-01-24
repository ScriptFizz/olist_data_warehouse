-- ID and review score for each reviewed order
-- Grain: 1 row per order_id

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_reviews` AS
SELECT
    order_id,
    ARRAY_AGG(score ORDER BY creation_date DESC LIMIT 1)[OFFSET(0)] AS score
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.reviews`
WHERE score IS NOT NULL
GROUP BY order_id;
