-- Grain: 1 row per (state, city)

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.bi_geo_kpi` AS
SELECT
    COALESCE(d.state_code, s.state_code) AS state_code,
    COALESCE(d.state_full, s.state_full) AS state_full,
    COALESCE(d.state_iso, s.state_iso) AS state_iso,
    COALESCE(d.city, s.city) AS city,
    COALESCE(d.city_location, s.city_location) AS city_location,
    COALESCE(d.total_unique_customers, 0) AS total_customers,
    COALESCE(s.total_sellers, 0) AS total_seller,
    COALESCE(d.total_orders_placed, 0) AS total_orders_placed,
    COALESCE(s.total_orders_sold, 0) AS total_orders_sold,
    COALESCE(d.total_items_ordered, 0) AS total_items_ordered,
    COALESCE(s.total_items_sold, 0) AS total_items_sold,
    COALESCE(d.total_revenue_demand, 0) AS total_revenue_demand,
    COALESCE(s.total_revenue_supply, 0) AS total_revenue_supply,
    COALESCE(s.total_revenue_supply, 0) - COALESCE(d.total_revenue_demand, 0) AS revenue_supply_demand_gap,
    COALESCE(d.pct_delayed, 0) AS pct_delayed
FROM `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_geo_demand` d
FULL OUTER JOIN `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_geo_supply` s
    ON d.state_code = s.state_code 
    AND d.state_full = s.state_full
    AND d.state_iso = s.state_iso
    AND d.city = s.city
    AND s.city_location = d.city_location;


