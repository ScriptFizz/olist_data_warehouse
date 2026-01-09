CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.seller_metrics` AS
WITH seller_order_agg AS (
SELECT
    order_id,
    seller_id,
    COUNT(*) AS n_items,
    SUM(total_item_value) AS seller_order_value
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items`
GROUP BY seller_id, order_id    
)
SELECT
    seller_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(n_items) AS total_items_sold,
    SUM(seller_order_value) AS total_revenue,
    AVG(seller_order_value) AS avg_order_value
FROM seller_order_agg
GROUP BY seller_id;
