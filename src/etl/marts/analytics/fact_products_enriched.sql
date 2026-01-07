-- Products with logistic, pricings and reviews stats.
-- Grain: one row per product

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.fact_products_enriched` AS
WITH single_item_orders AS (
    SELECT
        o.order_id,
        o.n_products
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
    WHERE o.n_products = 1
),
oi_enriched AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.price,
        oi.freight_value
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
    JOIN single_item_orders sio ON oi.order_id = sio.order_id
)
SELECT
    oi.product_id,
    p.category,

    -- Review
    COUNT(r.score) AS n_reviews,
    AVG(r.score) AS avg_score,

    -- Logistic
    AVG(o.delivery_time) AS avg_delivery_time,
    AVG(o.delay) AS avg_delay,
    AVG(CASE WHEN o.delay > 0 THEN 1 ELSE 0 END) AS pct_delayed,

    --Economics
    AVG(oi.price) AS avg_price,
    AVG(oi.freight_value) AS avg_freight_value

FROM oi_enriched oi
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o ON oi.order_id = o.order_id
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.reviews_agg` r ON oi.order_id = r.order_id
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_products` p ON oi.product_id = p.product_id
GROUP by oi.product_id, p.category;
