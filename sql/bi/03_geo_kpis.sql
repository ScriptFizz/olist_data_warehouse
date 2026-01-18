

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_kpis`
CLUSTER BY state, city AS
SELECT
    COALESCE(d.state, s.state) AS state,
    COALESCE(d.state_full, s.state_full) AS state_full,
    COALESCE(d.state_iso, s.state_iso) AS state_iso,
    COALESCE(d.city, s.city) AS city,
    COALESCE(d.city_location, s.city_location) AS city_location,
    
    -- Demand-side
    d.total_orders_placed,
    d.total_items_ordered,
    d.total_revenue_demand,
    d.avg_total_revenue_demand,
    
    -- Supply-side
    s.total_orders_sold,
    s.total_items_sold,
    s.total_revenue AS total_revenue_supply,
    s.avg_total_revenue_sold,
    
    -- Derived KPIs
    d.total_revenue_demand - s.total_revenue AS revenue_supply_demand_gap
    
FROM `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_demand_kpis` d
FULL OUTER JOIN `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.geo_supply_kpis` s
    ON d.state = s.state AND d.city = s.city;
