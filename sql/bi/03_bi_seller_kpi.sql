-- Grain: 1 row per seller_id

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.bi_seller_kpi` AS
SELECT
    dse.seller_id,
    dse.city,
    dse.state_code,
    dse.state_iso,
    dst.state_full,
    rs.total_orders,
    rs.total_items_sold,
    rs.total_revenue,
    ks.avg_delivery_time,
    ks.pct_delayed,
    ks.avg_review_score,
    ss.seller_segment
    
    
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_seller` dse
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_state` dst
    ON dse.state_code = dst.state_code
LEFT JOIN `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.rollup_seller_orders` rs
    ON dse.seller_id = rs.seller_id
LEFT JOIN `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_seller` ks
    ON dse.seller_id = ks.seller_id
LEFT JOIN `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.sem_seller_segment` ss
    ON dse.seller_id = ss.seller_id;
