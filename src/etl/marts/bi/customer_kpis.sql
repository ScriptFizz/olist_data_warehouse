CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ BI_DATASET_ID }}.customer_kpis` AS
SELECT
    ce.customer_unique_id,
    ce.recency_days,
    ce.frequency,
    ce.monetary_value,
    ce.is_inactive,
    (c_rfm.r_score + c_rfm.f_score + c_rfm.m_score) / 3 AS rfm
FROM `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_enriched` ce
JOIN `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_rfm` c
ON ce.customer_unique_id = c_rfm.customer_unique_id;
