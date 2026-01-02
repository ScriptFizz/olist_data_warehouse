-- Customer data with churn
-- Grain: one row per customer

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_churn` AS
WITH cutoff_date AS (
    SELECT 
        MAX(o.purchase_ts) AS cutoff_date
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
)
SELECT
    c.customer_uid AS customer_unique_id,
    
    -- Dates
    MIN(o.purchase_ts) AS first_purchase_date,
    MAX(o.purchase_ts) AS last_purchase_date,
    
    -- Recency
    DATE_DIFF(cod.cutoff_date, MAX(o.purchase_ts), DAY) AS recency_days,
    
    -- Volume
    COUNT(DISTINCT o.order_id) AS frequency,
    COUNT(o.order_id) AS total_items,
    SUM(o.total_order_value) AS monetary_value,
    CASE
       WHEN DATE_DIFF(cod.cutoff_date, MAX(o.purchase_ts), DAY) > 90
       THEN TRUE ELSE FALSE
    END AS has_churned
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c ON o.customer_id = c.customer_id
CROSS JOIN cutoff_date cod
GROUP BY customer_unique_id, cod.cutoff_date;


-- CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_churn` AS
-- WITH cutoff_date AS (
    -- SELECT 
        -- MAX(o.purchase_ts) AS cutoff_date
    -- FROM {{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.orders
-- ),
-- order_level AS (
   -- SELECT
       -- o.customer_uid,
       -- o.order_id,
       -- o.purchase_ts,
       -- SUM(oi.price + oi.freight_value) AS total_item_value
    -- FROM {{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.orders o
    -- JOIN {{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.order_items oi ON o.order_id = oi.order_id
-- )
-- SELECT
    -- c.customer_uid AS customer_unique_id,
    
    -- -- Dates
    -- MIN(ol.purchase_ts) AS first_purchase_date,
    -- MAX(ol.purchase_ts) AS last_purchase_date,
    
    -- -- Recency
    -- DATE_DIFF(cutoff_date.cutoff_date, MAX(ol.purchase_ts), DAY) AS recency_days,
    
    -- -- Volume
    -- COUNT(DISTINCT order_id) AS total_orders,
    -- COUNT(ol.order_id) AS total_items,
    -- SUM(ol.total_item_value) AS monetary_value,
    -- CASE
       -- WHEN DATE_DIFF(cutoff_date.cutoff_date, MAX(ol.purchase_ts), DAY) > 90
       -- THEN TRUE ELSE FALSE
    -- END AS has_churned
-- FROM {{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.orders o
-- JOIN {{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers c ON o.customer_id = c.customer_id
-- CROSS JOIN cutoff_date cod
-- GROUP BY customer_unique_id, cod.cutoff_date
