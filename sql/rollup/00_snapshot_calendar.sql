-- Monthly snapshot
-- Grain: 1 row per month

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ ROLLUP_DATASET_ID }}.snapshot_calendar` AS
WITH date_bounds AS (
    SELECT 
        DATE_TRUNC(MIN(date), MONTH) AS min_date,
        DATE_TRUNC(MAX(date), MONTH) AS max_date
    FROM `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_date`
)
SELECT
    d AS snapshot_date,
    CAST(FORMAT_DATE('%Y%m', d) AS INT64) AS snapshot_month_id
FROM date_bounds,
UNNEST(
    GENERATE_DATE_ARRAY(min_date, max_date, INTERVAL 1 MONTH)
) AS d
ORDER BY snapshot_date;
