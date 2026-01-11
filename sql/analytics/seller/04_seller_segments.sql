CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.seller_segments` AS
WITH seller_base AS (
    SELECT 
        sm.seller_id,
        sm.total_orders,
        sm.total_revenue,
        sm.avg_order_value,
        slm.avg_delivery_time,
        slm.pct_delayed,
        sqm.avg_review_score,
        sqm.n_reviews
    FROM `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.seller_metrics` sm
    LEFT JOIN `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.seller_logistics_metrics` slm
    ON sm.seller_id = slm.seller_id
    LEFT JOIN `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.seller_quality_metrics` sqm
    ON sm.seller_id = sqm.seller_id
)
SELECT
    seller_id,
    
    CASE
    
        -- High volume sellers
        WHEN total_orders >= 1000
            THEN 'High-volume seller'
        
        -- High quality niche sellers
        WHEN avg_review_score >= 4.5 AND total_orders BETWEEN 50 AND 999
            THEN 'High-quality niche seller'
        
        -- Logistic-risk sellers
        WHEN pct_delayed >= 0.30 AND total_orders >= 50
            THEN 'Logistic-risk seller'
        
        -- Long-tail sellers
        WHEN total_orders < 50
            THEN 'Long-tail seller'
        
        -- Standard sellers
        ELSE 'Standard seller'
    END AS seller_segment,
    
    'v1_rule_based_2016_01' AS segmentation_version

FROM seller_base;
    
