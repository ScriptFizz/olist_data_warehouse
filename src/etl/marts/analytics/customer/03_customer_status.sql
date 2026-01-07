CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_status` AS
WITH params AS (
    SELECT 180 AS inactivity_threshold_days
)
SELECT
    cm.customer_unique_id,
    p.inactivity_threshold_days AS inactivity_threshold_days,
    cm.recency_days > p.inactivity_threshold_days AS is_inactive
FROM `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_metrics` cm
CROSS JOIN params p;
