-- Grain: 1 row per seller
-- For multi-seller orders, delivery time and delay are attributed fully to each seller
-- (total across sellers != total across orders)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_seller_orders` AS
WITH seller_orders AS (
SELECT 
    oi.seller_id,
    oi.order_id,
    SUM(oi.freight_value) AS order_freight_value,
    SUM(oi.item_total_value) AS order_total_value,
    COUNT(oi.order_item_id) AS seller_items_in_order, 
    MAX(o.delivery_time) AS delivery_time,
    MAX(o.delay) AS delay,
    MAX(r.score) AS review_score
    
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
    ON oi.order_id = o.order_id
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_reviews` r
    ON oi.order_id = r.order_id
GROUP BY
    oi.seller_id, 
    oi.order_id
)
SELECT
    s.seller_id,
    COUNT(DISTINCT so.order_id) AS total_orders,
    COALESCE(SUM(so.seller_items_in_order), 0) AS total_items_sold,
    COALESCE(SUM(so.order_total_value), 0) AS total_revenue,
    COALESCE(SUM(so.delivery_time), 0) AS sum_delivery_time,
    COALESCE(SUM(CASE WHEN so.delivery_time IS NOT NULL THEN 1 ELSE 0 END), 0) AS cnt_delivery_time,
    COALESCE(SUM(CASE WHEN so.delay > 0 THEN so.delay ELSE 0 END), 0) AS sum_delay_time,
    COALESCE(SUM(CASE WHEN so.delay > 0 THEN 1 ELSE 0 END), 0) AS cnt_delay_time,
    COALESCE(SUM(so.order_freight_value), 0) AS sum_freight_value,
    COALESCE(SUM(so.review_score), 0) AS sum_review_score,
    COALESCE(COUNT(so.review_score), 0) AS cnt_review_score
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_seller` s
LEFT JOIN seller_orders so
    ON s.seller_id = so.seller_id
GROUP BY s.seller_id;

