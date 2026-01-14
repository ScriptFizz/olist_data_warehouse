CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.snapshot_calendar` AS
WITH bounds AS (
    SELECT
        DATE '2016-01-01' AS start_date,
        DATE_TRUNC(
            DATE(MAX(purchase_ts)),
            MONTH
        ) AS end_date
    FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.orders`
)
SELECT
    d AS snapshot_date
FROM bounds, 
UNNEST(
    GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 MONTH)
) d;
