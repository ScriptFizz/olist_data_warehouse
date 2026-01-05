
CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_cohort` AS
WITH first_second_purchase AS (
SELECT
     customer_unique_id,
     ARRAY_AGG(purchase_ts ORDER BY purchase_ts) AS purchases,
     MIN(purchase_ts) AS first_purchase_date,
     DATE_TRUNC(MIN(purchase_ts), MONTH) AS cohort_month
FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_orders`
GROUP BY customer_unique_id
),
customer_purchase_milestones AS (
SELECT
     customer_unique_id,
     cohort_month,
     first_purchase_date,
     -- Second purchase if it exists
     purchases[SAFE_OFFSET(1)] AS second_purchase_date
FROM first_second_purchase
)
SELECT
    cohort_month,
    COUNT(*) AS cohort_size,
    COUNTIF(
        second_purchase_date IS NULL
        OR DATE_DIFF(second_purchase_date, first_purchase_date, DAY) > 365
    ) AS churned,
    SAFE_DIVIDE(
        COUNTIF(
            second_purchase_date IS NULL
            OR DATE_DIFF(second_purchase_date, first_purchase_date, DAY) > 365
        ),
        COUNT(*)
    ) AS churn_rate
FROM customer_purchase_milestones
GROUP BY cohort_month
ORDER BY cohort_month;
