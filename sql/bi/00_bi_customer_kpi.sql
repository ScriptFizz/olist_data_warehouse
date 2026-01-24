-- Grain: 1 row per (snapshot_date, customer_unique_id)

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.bi_customer_kpi` AS
SELECT
    kc.snapshot_date,
    kc.customer_unique_id,
    kc.recency_days,
    kc.frequency,
    kc.monetary_value,
    cst.is_inactive,
    csg.monetary_segment,
    csg.behavior_segment

FROM `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_customer` kc
JOIN `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.sem_customer_status` cst
    ON kc.customer_unique_id = cst.customer_unique_id
    AND kc.snapshot_date = cst.snapshot_date
JOIN `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.sem_customer_segment` csg    
    ON kc.customer_unique_id = csg.customer_unique_id
    AND kc.snapshot_date = csg.snapshot_date;    
    
