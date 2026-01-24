-- Sellers ID and locations
-- Grain: 1 row per seller_id

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_seller` AS
SELECT
    s.seller_id,
    INITCAP(s.city) AS city,
    UPPER(s.state) AS state_code,
    ds.state_iso AS state_iso
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.sellers` s
LEFT JOIN `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_state` ds
ON UPPER(s.state) = ds.state_code;
