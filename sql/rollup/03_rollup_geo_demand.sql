-- Additive aggregates of demand data for geographical locations
-- Grain: 1 row per (state, city)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_geo_demand` AS
WITH customer_geo AS (
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.city,
    CONCAT(c.city, ', ', s.state_full, ', Brazil') AS city_location,
    c.state_code,
    c.state_iso,
    s.state_full
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_customer` c
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_state` s
ON c.state_code = s.state_code
)
SELECT
    cg.state_code,
    cg.state_full,
    cg.state_iso,
    cg.city,
    cg.city_location,
    COUNT(DISTINCT cg.customer_unique_id) AS total_unique_customers,
    COUNT(DISTINCT o.order_id) AS total_orders_placed,
    SUM(o.basket_size) AS total_items_ordered,
    SUM(o.total_order_value) AS total_revenue_demand,
    SUM(CASE WHEN o.delay > 0 THEN 1 ELSE 0 END) AS total_orders_delayed
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
JOIN customer_geo cg 
    ON o.customer_id = cg.customer_id
GROUP BY 
    cg.state_code, 
    cg.state_full, 
    cg.state_iso, 
    cg.city, 
    cg.city_location;

