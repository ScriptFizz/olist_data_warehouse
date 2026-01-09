-- Orders with logistics/pricings data
-- Grain: one row per order

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` AS
WITH order_items_agg AS (
SELECT
    oi.order_id,
    COUNT(DISTINCT oi.product_id) AS n_products,
    COUNT(oi.product_id) AS basket_size,
    CASE
    WHEN COUNT(DISTINCT oi.product_id) = 1
        THEN ANY_VALUE(oi.product_id)
        ELSE "multi-items"
    END AS product,
    SUM(oi.price) AS price,
    SUM(oi.freight_value) AS freight_value,
    SUM(oi.price + oi.freight_value) AS total_order_value
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.order_items` oi
GROUP BY oi.order_id
)
SELECT
    o.order_id,
    o.customer_id,
    c.customer_uid AS customer_unique_id,
    o.purchase_ts,
    CASE
    WHEN o.order_status = "delivered" AND o.delivery_customer_ts IS NOT NULL
        THEN TIMESTAMP_DIFF(o.delivery_customer_ts, o.purchase_ts, DAY)
        ELSE NULL
    END AS delivery_time,
    CASE
    WHEN o.order_status = "delivered" AND o.delivery_customer_ts IS NOT NULL
        THEN TIMESTAMP_DIFF(o.delivery_customer_ts, o.estimated_delivery_ts, DAY)
        ELSE NULL
    END AS delay,
    oia.n_products,
    oia.basket_size,
    oia.product,
    oia.price,
    oia.freight_value,
    oia.total_order_value,
    o.order_status
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.orders` o
JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c
ON o.customer_id = c.customer_id
LEFT JOIN order_items_agg oia
ON o.order_id = oia.order_id;
