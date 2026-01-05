-- Product data view
-- Grain: one product per row

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_products` AS
SELECT 
     oi.order_id,
     oi.product_id,
     p.category
     
     -- Economics
     oi.price,
     oi.freight_value,
     oi.total_item_value,
     
     -- Order context
     o.purchase_ts,
     o.delivery_time,
     o.delay,
     o.order_status,
     
     -- Review (nullable)
     r.score AS review_score
     
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
ON oi.order_id = o.order_id
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.reviews_agg` r
ON oi.order_id = r.order_id
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_products` p
ON oi.product_id = p.product_id;
