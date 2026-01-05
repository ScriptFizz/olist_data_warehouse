-- Customer data with EPCIR
-- Grain: one row per customer

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_enriched` AS
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
    END AS is_inactive
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c ON o.customer_id = c.customer_id
CROSS JOIN cutoff_date cod
GROUP BY customer_unique_id, cod.cutoff_date;

