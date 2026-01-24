-- Products with categories in English
-- Grain: one product per row

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_product` AS
SELECT
    p.product_id,
    t.name_eng AS category
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.products` p
LEFT JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.translation` t
ON p.name = t.name_brz;
