-- Segments data for each seller
-- Grain: 1 row per (seller_id)

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.sem_seller_segment` AS
SELECT

    dse.seller_id,
    CASE
    
        -- High volume sellers
        WHEN rs.total_orders >= 1000
            THEN 'High-volume seller'
        
        -- High quality niche sellers
        WHEN ks.avg_review_score >= 4.5 AND rs.total_orders BETWEEN 50 AND 999
            THEN 'High-quality niche seller'
        
        -- Logistic-risk sellers
        WHEN ks.pct_delayed >= 0.30 AND rs.total_orders >= 50
            THEN 'Logistic-risk seller'
        
        -- Long-tail sellers
        WHEN rs.total_orders < 50
            THEN 'Long-tail seller'
        
        -- Standard sellers
        ELSE 'Standard seller'
    END AS seller_segment,
    
    'v1_rule_based_2016_01' AS segmentation_version

FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_seller` dse
LEFT JOIN `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_seller_orders` rs
    ON dse.seller_id = rs.seller_id
LEFT JOIN `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_seller` ks
    ON dse.seller_id = ks.seller_id;

