-- Snapshots of additive aggregates for each customer
-- Grain: 1 row per (snapshot_date, customer_unique_id)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_customer_orders` AS
WITH first_order AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC(MIN(DATE(o.purchase_ts)), MONTH) AS first_month
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_customer` c 
    JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
        ON c.customer_id = o.customer_id
    GROUP BY
        c.customer_unique_id
)
SELECT
    sc.snapshot_date,
    fo.customer_unique_id,

    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.n_products) AS total_items,
    SUM(o.total_order_value) AS total_revenue
FROM first_order fo
JOIN `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.snapshot_calendar` sc
    ON sc.snapshot_date >= fo.first_month
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
    ON o.customer_unique_id = fo.customer_unique_id
    AND DATE(o.purchase_ts) <= sc.snapshot_date
GROUP BY 
    sc.snapshot_date,
    fo.customer_unique_id;

