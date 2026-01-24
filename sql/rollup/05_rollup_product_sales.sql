-- Additive aggregate for products
-- Grain: 1 row per product_id
-- For multi-product orders, delivery time and delay are attributed fully to each product
-- (total across products != total across orders)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_product_sales` AS
WITH product_orders AS (
SELECT 
    oi.product_id,
    oi.order_id,
    SUM(oi.freight_value) AS order_freight_value,
    SUM(oi.item_price) AS order_price_value,
    SUM(oi.item_total_value) AS order_total_value,
    COUNT(oi.order_item_id) AS items_per_order,
    MAX(o.delivery_time) AS delivery_time,
    MAX(o.delay) AS delay,
    MAX(r.score) AS review_score
    
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
    ON oi.order_id = o.order_id
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_reviews` r
    ON oi.order_id = r.order_id
GROUP BY
    oi.product_id, 
    oi.order_id
)
SELECT
    p.product_id,
    COUNT(DISTINCT po.order_id) AS total_orders,
    COALESCE(SUM(po.items_per_order), 0) AS total_items_sold,
    COALESCE(SUM(po.order_total_value), 0) AS total_revenue,
    COALESCE(SUM(po.order_price_value), 0) AS sum_price,
    COALESCE(SUM(po.order_freight_value), 0) AS sum_freight,
    COALESCE(SUM(po.delivery_time), 0) AS sum_delivery_time,
    COALESCE(SUM(CASE WHEN po.delivery_time IS NOT NULL THEN 1 ELSE 0 END), 0) AS cnt_delivery_time,
    COALESCE(SUM(CASE WHEN po.delay > 0 THEN po.items_per_order ELSE 0 END), 0) AS delayed_items,
    COALESCE(SUM(CASE WHEN po.review_score IS NOT NULL THEN 1 ELSE 0 END), 0) AS cnt_reviews,
    COALESCE(SUM(po.review_score), 0) AS sum_review_score
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_product` p
LEFT JOIN product_orders po
    ON p.product_id = po.product_id
GROUP BY p.product_id;

