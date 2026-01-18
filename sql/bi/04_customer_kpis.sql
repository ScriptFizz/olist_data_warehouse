CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.customer_kpis`
CLUSTER BY customer_unique_id AS
SELECT
    cm.snapshot_date,
    cm.customer_unique_id,
    cm.recency_days,
    cm.frequency,
    cm.monetary_value,
    cs.is_inactive,
    csg.monetary_segment,
    csg.behavior_segment
FROM `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_metrics` cm
LEFT JOIN `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_status` cs
USING (snapshot_date, customer_unique_id)
LEFT JOIN `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_segments` csg
USING (snapshot_date, customer_unique_id);

