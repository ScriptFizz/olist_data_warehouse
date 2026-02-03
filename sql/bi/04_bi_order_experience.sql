-- Grain: 1 row per order_id

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ BI_ORDER_EXPERIENCE }}.bi_order_experience` AS
SELECT
    o.order_id,
    o.delivery_time AS delivery_days,
    r.score AS review_score,
    o.product,
    o.total_order_value AS total_revenue,
    o.delay > 0 AS is_delayed,
    c.city,
    c.state_code,
    c.state_iso

FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_reviews` r
    ON o.order_id = r.order_id

LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_customer` c   
    ON o.customer_id = c.customer_id;    
    
