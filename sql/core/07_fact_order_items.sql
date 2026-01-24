-- Seller and pricing data from each product ordered
-- Grain: 1 row per (order_id, order_item_id, product_id)

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ CORE_DATASET_ID }}.fact_order_items` AS
SELECT
     oi.order_id,
     oi.order_item_id,
     oi.product_id,
     oi.seller_id,
     oi.price AS item_price,
     oi.freight_value,
     oi.price + oi.freight_value AS item_total_value
FROM `{{ PROJECT_ID }}.{{ RAW_DATASET_ID }}.order_items` oi;
