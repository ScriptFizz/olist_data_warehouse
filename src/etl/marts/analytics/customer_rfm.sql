-- Customer data with churn
-- Grain: one row per customer

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_rfm` AS
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary_value,
    
    NTILE(5) OVER (ORDER BY recency_days ASC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary_value DESC) AS m_score

FROM `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_churn`;
