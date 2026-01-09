-- Grain: one row per order item

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` AS
SELECT
     oi.order_id,
     oi.product_id,
     oi.seller_id,
     oi.price,
     oi.freight_value,
     oi.price + oi.freight_value AS total_item_value
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.order_items` oi;
