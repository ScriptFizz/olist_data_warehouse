-- Customers ID and locations
-- Grain: 1 row per customer_id

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_customer` AS
SELECT
    c.customer_id,
    c.customer_uid AS customer_unique_id,
    INITCAP(c.city) AS city,
    UPPER(c.state) AS state_code,
    ds.state_iso AS state_iso
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.customers` c
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_state` ds
ON UPPER(c.state) = ds.state_code;
