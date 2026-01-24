-- KPIs/Ratio aggregates for each seller
-- Grain: 1 row per seller_id
-- For multi-seller orders, delivery time and delay are attributed fully to each seller
-- (total across sellers != total across orders)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_seller` AS
SELECT
    s.seller_id,
    SAFE_DIVIDE(
        s.sum_delivery_time,
        s.cnt_delivery_time
    ) AS avg_delivery_time,
    SAFE_DIVIDE(
        s.cnt_delay_time,
        s.total_orders
    ) AS pct_delayed,
    SAFE_DIVIDE(
        s.sum_review_score,
        s.cnt_review_score
    ) AS avg_review_score

FROM `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_seller_orders` s;

