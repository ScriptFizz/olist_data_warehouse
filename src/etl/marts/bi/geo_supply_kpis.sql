CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_supply_kpis`
CLUSTER BY state, city
AS
SELECT
    s.state,
    s.city,
    COUNT(DISTINCT oi.order_id) AS total_orders_sold,
    COUNT(oi.product_id) AS total_items_sold,
    SUM(oi.total_item_value) AS total_revenue,
    AVG(oi.total_item_value) AS avg_total_revenue_sold
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.sellers` s ON oi.seller_id = s.seller_id
GROUP BY s.state, s.city;
