CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.seller_quality_metrics` AS
WITH seller_orders AS (
SELECT DISTINCT
    seller_id,
    order_id
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items`
)
SELECT
    so.seller_id AS seller_id,
    AVG(r.score) AS avg_review_score,
    COUNT(r.score) AS n_reviews
FROM seller_orders so
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.reviews_agg` r ON so.order_id = r.order_id
GROUP BY seller_id;
