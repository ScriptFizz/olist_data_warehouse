CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_demand_kpis` AS
WITH customer_geo AS (
SELECT
    c.customer_id,
    c.state,
    c.city,
    snm.state_full,
    CONCAT('BR-', c.state) AS state_iso,
    CONCAT(c.city, ', ', snm.state_full, ', Brazil') AS city_location
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.state_names_map` snm 
ON c.state = snm.uf
)
SELECT
    cg.state,
    cg.state_full,
    cg.state_iso,
    cg.city,
    cg.city_location,
    COUNT(DISTINCT o.order_id) AS total_orders_placed,
    SUM(o.basket_size) AS total_items_ordered,
    SUM(o.total_order_value) AS total_revenue_demand,
    AVG(o.total_order_value) AS avg_total_revenue_demand
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
JOIN customer_geo cg ON o.customer_id = cg.customer_id
GROUP BY cg.state, cg.state_full, cg.state_iso, cg.city, cg.city_location;


-- CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_demand_kpis`
-- CLUSTER BY state, city AS
-- WITH customer_geo AS (
-- SELECT
    -- c.customer_id,
    -- c.state,
    -- c.city,
    -- snm.state_full,
    -- CONCAT('BR-', c.state) AS state_iso,
    -- CONCAT(c.city, ', ', snm.state_full, ', Brazil') AS city_location
-- FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c
-- JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.state_names_map` snm 
-- ON c.state = snm.uf
-- )
-- SELECT
    -- cg.state,
    -- cg.state_full,
    -- cg.state_iso,
    -- cg.city,
    -- cg.city_location,
    -- COUNT(DISTINCT o.order_id) AS total_orders_placed,
    -- SUM(o.basket_size) AS total_items_ordered,
    -- SUM(o.total_order_value) AS total_revenue_demand,
    -- AVG(o.total_order_value) AS avg_total_revenue_demand
-- FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
-- JOIN customer_geo cg ON o.customer_id = cg.customer_id
-- GROUP BY cg.state, cg.state_full, cg.state_iso, cg.city, cg.city_location;
