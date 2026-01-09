CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_behavior_features` AS
WITH payments_agg AS (
    SELECT order_id,
    MAX(installments) AS installments
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.payments`
GROUP BY order_id
)
SELECT
    sc.snapshot_date,
    o.customer_unique_id,
    AVG(o.n_products) AS items_per_order,
    AVG(p.installments) AS avg_installments,
    AVG(p.installments) > 1 AS uses_installments,
    AVG(o.delivery_time) AS avg_delivery_time,
    AVG(CASE WHEN o.delay > 0 THEN 1 ELSE 0 END) AS pct_delayed,
    AVG(o.freight_value / NULLIF(o.total_order_value, 0)) AS freight_ratio

FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
JOIN payments_agg p
ON o.order_id = p.order_id
JOIN `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.snapshot_calendar` sc
ON DATE(o.purchase_ts) <= sc.snapshot_date
GROUP BY
    sc.snapshot_date,
    o.customer_unique_id;


-- CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_behavior_features` AS
-- WITH payments_agg AS (
    -- SELECT order_id,
    -- MAX(installments) AS installments
-- FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.payments`
-- GROUP BY order_id
-- )
-- ,order_enriched AS (
-- SELECT 
    -- o.order_id,
    -- o.customer_unique_id,
    -- o.n_products,
    -- o.freight_value,
    -- o.total_order_value,
    -- o.delivery_time,
    -- o.delay,
    -- p.installments
-- FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
-- JOIN payments_agg p ON o.order_id = p.order_id
-- )
-- SELECT
    -- oe.customer_unique_id,
    -- AVG(oe.n_products) AS items_per_order,
    -- AVG(oe.installments) AS avg_installments,
    -- AVG(oe.installments) > 1 AS uses_installments,
    -- AVG(oe.delivery_time) AS avg_delivery_time,
    -- AVG(CASE WHEN oe.delay > 0 THEN 1 ELSE 0 END) AS pct_delayed,
    -- AVG(oe.freight_value / NULLIF(oe.total_order_value, 0)) AS freight_ratio

-- FROM order_enriched oe
-- GROUP BY oe.customer_unique_id;
