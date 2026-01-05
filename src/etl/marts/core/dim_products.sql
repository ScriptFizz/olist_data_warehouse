-- products with categories in English

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.dim_products` AS
SELECT
    p.product_id,
    t.name_eng AS category
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.products` p
LEFT JOIN `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.translation` t
ON p.name = t.name_brz;
