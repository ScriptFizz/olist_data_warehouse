-- Snapshots of KPI/ratio aggegates for each customer
-- Grain: 1 row per (snapshot_date, customer_unique_id)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_customer` AS
WITH first_order AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC(MIN(DATE(o.purchase_ts)), MONTH) AS first_month
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_customer` c 
    JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
        ON c.customer_id = o.customer_id
    GROUP BY
        c.customer_unique_id
),
payment_agg AS (
    SELECT
        order_id,
        MAX(installments) AS order_installments 
    FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.payments`
    GROUP BY order_id
)
SELECT
    sc.snapshot_date,
    fo.customer_unique_id,
    
    -- Recency
    DATE_DIFF(
        sc.snapshot_date, 
        DATE(MAX(o.purchase_ts)), 
        DAY
        ) AS recency_days,
        
    -- Frequency
    COUNT(DISTINCT o.order_id) AS frequency,
    
    -- Monetary
    SUM(o.total_order_value) AS monetary_value,
    
    AVG(o.total_order_value) AS avg_order_value,
    
    SAFE_DIVIDE(
        SUM(o.n_products),
        COUNT(DISTINCT o.order_id)
    ) AS items_per_order,
    
    SAFE_DIVIDE(
        COUNTIF(o.delay > 0),
        COUNT(DISTINCT o.order_id)
    ) AS pct_delayed,
    
    SAFE_DIVIDE(
       SUM(COALESCE(p.order_installments, 1)),
        COUNT(DISTINCT o.order_id)
    ) AS avg_installments,
    
    MAX(
        CASE
        WHEN p.order_installments > 1
        THEN 1 ELSE 0
        END
    ) = 1 AS uses_installments,
    
    SAFE_DIVIDE(
        SUM(o.freight_value),
        SUM(o.total_order_value)
    ) AS freight_ratio
    
FROM first_order fo
JOIN `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.snapshot_calendar` sc
    ON sc.snapshot_date >= fo.first_month
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders` o
    ON o.customer_unique_id = fo.customer_unique_id
    AND DATE(o.purchase_ts) <= sc.snapshot_date
LEFT JOIN payment_agg p
    ON o.order_id = p.order_id
GROUP BY 
    sc.snapshot_date,
    fo.customer_unique_id;
