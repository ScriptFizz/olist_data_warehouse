CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_supply_kpis` AS
WITH seller_geo AS (
    SELECT 
        s.seller_id,
        s.state,
        s.city,
        snm.state_full,
        CONCAT('BR-', s.state) AS state_iso,
        CONCAT(s.city, ', ', snm.state_full, ', Brazil') AS city_location
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.sellers` s
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.state_names_map` snm 
ON s.state = snm.uf
)
SELECT
    sg.state,
    sg.state_full,
    sg.state_iso,
    sg.city,
    sg.city_location,
    COUNT(DISTINCT oi.order_id) AS total_orders_sold,
    COUNT(oi.product_id) AS total_items_sold,
    SUM(oi.total_item_value) AS total_revenue,
    AVG(oi.total_item_value) AS avg_total_revenue_sold
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
JOIN seller_geo sg ON oi.seller_id = sg.seller_id
GROUP BY sg.state, sg.state_full, sg.state_iso, sg.city, sg.city_location;


-- CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_supply_kpis`
-- CLUSTER BY state, city
-- AS
-- WITH seller_geo AS (
    -- SELECT 
        -- s.seller_id,
        -- s.state,
        -- s.city,
        -- snm.state_full,
        -- CONCAT('BR-', s.state) AS state_iso,
        -- CONCAT(s.city, ', ', snm.state_full, ', Brazil') AS city_location
-- FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.sellers` s
-- JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.state_names_map` snm 
-- ON s.state = snm.uf
-- )
-- SELECT
    -- sg.state,
    -- sg.state_full,
    -- sg.state_iso,
    -- sg.city,
    -- sg.city_location,
    -- COUNT(DISTINCT oi.order_id) AS total_orders_sold,
    -- COUNT(oi.product_id) AS total_items_sold,
    -- SUM(oi.total_item_value) AS total_revenue,
    -- AVG(oi.total_item_value) AS avg_total_revenue_sold
-- FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
-- JOIN seller_geo sg ON oi.seller_id = sg.seller_id
-- GROUP BY sg.state, sg.state_full, sg.state_iso, sg.city, sg.city_location;
