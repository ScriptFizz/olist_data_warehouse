-- Additive aggregates of supply data for geographical locations
-- Grain: 1 row per (state, city)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_geo_supply` AS
WITH seller_geo AS (
    SELECT 
        se.seller_id,
        se.state_code,
        se.city,
        st.state_full,
        se.state_iso,
        CONCAT(se.city, ', ', st.state_full, ', Brazil') AS city_location
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_seller` se
JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_state` st 
ON se.state_code = st.state_code
)
SELECT
    sg.state_code,
    sg.state_full,
    sg.state_iso,
    sg.city,
    sg.city_location,
    COUNT(DISTINCT sg.seller_id) AS total_sellers,
    COUNT(DISTINCT oi.order_id) AS total_orders_sold,
    COUNT(oi.product_id) AS total_items_sold,
    SUM(oi.item_total_value) AS total_revenue_supply
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` oi
JOIN seller_geo sg 
    ON oi.seller_id = sg.seller_id
GROUP BY 
    sg.state_code, 
    sg.state_full, 
    sg.state_iso, 
    sg.city, 
    sg.city_location;

