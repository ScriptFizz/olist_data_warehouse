-- Daily aggregated data
-- Grain: one row per date

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.daily_kpis` 
PARTITION BY order_date
AS 
WITH orders_daily AS (
    SELECT
        DATE(purchase_ts) AS order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_order_value) AS total_revenue,
        AVG(total_order_value) AS avg_order_value,
        AVG(delivery_time) AS avg_delivery_time.
        AVG(CASE WHEN delay > 0 THEN 1 ELSE 0 END) AS pct_delayed
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders`
    GROUP BY order_date
),
reviews_daily AS (
    SELECT
        DATE(o.purchase_ts) AS order_date,
        AVG(r.score) AS avg_review_score
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
    JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.reviews_agg` r
    ON o.order_id = r.order_id
    GROUP BY order_date
)
SELECT
    o.order_date,
    o.total_orders,
    o.total_revenue,
    o.avg_order_value,
    o.avg_delivery_time,
    o.pct_delayed,
    r.avg_review_score
FROM orders_daily o
LEFT JOIN reviews_daily r
USING (order_date);
