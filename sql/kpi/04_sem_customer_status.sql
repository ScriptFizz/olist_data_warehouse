-- Activity segments data for each customer
-- Grain: 1 row per (snapshot_date, customer_unique_id)

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.sem_customer_status` AS
WITH params AS (
    SELECT 180 AS inactivity_threshold_days
)
SELECT
    kc.snapshot_date,
    kc.customer_unique_id,
    p.inactivity_threshold_days AS inactivity_threshold_days,
    kc.recency_days > p.inactivity_threshold_days AS is_inactive
FROM `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_customer` kc
CROSS JOIN params p;

