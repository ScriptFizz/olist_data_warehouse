CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_demand_kpis`
CLUSTER BY state, city AS
SELECT
    c.state,
    c.city,
    COUNT(DISTINCT o.order_id) AS total_orders_placed,
    SUM(o.basket_size) AS total_items_ordered,
    SUM(o.total_order_value) AS total_revenue_demand,
    AVG(o.total_order_value) AS avg_total_revenue_demand
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c ON o.customer_id = c.customer_id
GROUP BY c.state, c.city;
