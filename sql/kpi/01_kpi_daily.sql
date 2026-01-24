-- Volume, pricing and review aggregates for each date
-- Grain: 1 row per date

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_daily` AS
WITH orders_enriched AS (
SELECT
    o.order_id,
    DATE(o.purchase_ts) AS order_date,
    o.delivery_time,
    o.delay,
    o.total_order_value,
    r.score as review_score
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_reviews` r
    ON o.order_id = r.order_id
)    
SELECT
    d.date,
    COUNT(DISTINCT oe.order_id) AS total_orders,
    SUM(oe.total_order_value) AS total_revenue,
    AVG(oe.total_order_value) AS avg_order_value,
    SAFE_DIVIDE(
        SUM(oe.delivery_time),
        COUNTIF(oe.delivery_time > 0)
    ) AS avg_delivery_time,
    SAFE_DIVIDE(
        COUNTIF(oe.delay > 0),
        COUNT(DISTINCT oe.order_id)
    ) AS pct_delayed,
    SAFE_DIVIDE(
        SUM(oe.review_score),
        COUNTIF(oe.review_score IS NOT NULL)
    ) AS avg_review_score

FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_date` d
LEFT JOIN orders_enriched oe
    ON d.date = oe.order_date
GROUP BY d.date
ORDER BY d.date DESC;

