CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.seller_logistics_metrics` AS
WITH seller_orders AS (
SELECT DISTINCT
    seller_id,
    order_id
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items`
)
SELECT
    so.seller_id AS seller_id,
    AVG(o.delivery_time) AS avg_delivery_time,
    AVG(CASE WHEN o.delay > 0 THEN 1 ELSE 0 END) AS pct_delayed
FROM seller_orders so
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o ON so.order_id = o.order_id
GROUP BY seller_id;
