CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.orders_revenue_by_location` AS
SELECT
    s.state,
    s.city,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.product_id) AS n_items,
    SUM(oi.total_item_value) AS total_revenue
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.sellers` s ON oi.seller_id = s.seller_id
GROUP BY s.state, s.city;
