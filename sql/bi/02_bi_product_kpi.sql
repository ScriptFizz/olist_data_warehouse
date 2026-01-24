-- Grain: 1 row per product_id
-- kpi_product is derived exclusively from rollup_product_sales
-- No additional filters or joins applied

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.bi_product_kpi` AS
SELECT
    dp.product_id,
    dp.category,
    kp.avg_price,
    kp.avg_freight,
    kp.avg_delivery_time,
    kp.pct_delayed,
    kp.avg_review_score,
    rp.total_orders,
    rp.total_items_sold,
    rp.total_revenue,
    rp.cnt_reviews
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_product` dp
LEFT JOIN `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_product` kp
    ON dp.product_id = kp.product_id
LEFT JOIN `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_product_sales` rp
    ON dp.product_id = rp.product_id;


