-- Orders with review data
-- Grain: one row per order

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.fact_orders_enriched` AS
SELECT
o.order_id,
o.product,
c.state AS state,
o.order_status AS status,
o.delivery_time,
o.delay,
o.price,
o.freight_value,
o.basket_size,
o.n_products,
r.score AS score
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c ON o.customer_id = c.customer_id
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.reviews_agg` r ON o.order_id = r.order_id
WHERE r.score IS NOT NULL;
-- GROUP BY o.order_id;

