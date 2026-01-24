-- KPIs/Ratio aggregates for each product sold
-- Grain: 1 row per product_id
-- For multi-product orders, delivery time and delay are attributed fully to each product
-- (total across product != total across orders)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_product` AS
SELECT
    product_id,
    SAFE_DIVIDE(
        sum_price,
        total_items_sold
    ) AS avg_price,
    SAFE_DIVIDE(
        sum_freight,
        total_items_sold
    ) AS avg_freight,
    SAFE_DIVIDE(
        sum_delivery_time,
        cnt_delivery_time
    ) AS avg_delivery_time,
    SAFE_DIVIDE(
        delayed_items,
        total_items_sold
    ) AS pct_delayed,
    SAFE_DIVIDE(
        sum_review_score,
        cnt_reviews
    ) AS avg_review_score

FROM `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_product_sales` s;

