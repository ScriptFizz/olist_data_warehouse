-- Product table with aggregate data
-- Grain: one row per product-category

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.product_kpis`
CLUSTER BY category
AS
SELECT
    product_id,
    category,

    -- Volume
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items_sold,

    -- Revenue
    SUM(total_item_value) AS total_revenue,
    AVG(price) AS avg_price,
    AVG(freight_value) AS avg_freight_value,

    -- Logistics
    AVG(delivery_time) AS avg_delivery_time,
    AVG(CASE WHEN delay > 0 THEN 1 ELSE 0 END) AS pct_delayed,

    -- Reviews
    COUNT(review_score) AS n_reviews,
    AVG(review_score) AS avg_review_score

FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_products`
GROUP BY product_id, category;
