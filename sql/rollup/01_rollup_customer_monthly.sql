-- Grain: 1 row per (customer_unique_id, year_month)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_customer_monthly` AS
SELECT
    c.customer_unique_id,
    FORMAT_DATE('%Y%m', DATE(o.purchase_ts)) AS year_month,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(o.n_products) AS n_items,
    SUM(o.total_order_value) AS revenue
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_customer` c
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
    ON c.customer_id = o.customer_id
GROUP BY
    customer_unique_id,
    year_month;
