-- Grain: 1 row per date
-- date range established dynamically from orders timestamps

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_date` AS
WITH date_bounds AS (
    SELECT
        MIN(d) AS min_date,
        MAX(d) AS max_date,
    FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.orders`,
    UNNEST([
        DATE(purchase_ts),
        DATE(approval_ts),
        DATE(delivery_carrier_ts),
        DATE(delivery_customer_ts)
    ]) AS d
    WHERE d IS NOT NULL
),
calendar AS (
    SELECT
        date
    FROM date_bounds,
    UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS date
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_id,
    date,
    EXTRACT(YEAR FROM date) AS year,    
    EXTRACT(ISOYEAR FROM date) AS iso_year,
    EXTRACT(MONTH FROM date) AS month,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(ISOWEEK FROM date) AS iso_week,
    EXTRACT(DAY FROM date) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    FORMAT_DATE('%A', date) AS day_name,
    EXTRACT(DAYOFWEEK FROM date) IN (1, 7) AS is_weekend,
    date = DATE_TRUNC(date, MONTH) AS is_month_start,
    date =  LAST_DAY(date) AS is_month_end

FROM calendar
ORDER BY date;
