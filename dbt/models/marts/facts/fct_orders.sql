-- Grain: exactly one row per order_id.
--
-- Additive measures:
--   item_count
--   distinct_product_count (not additive across orders if products repeat)
--   item_subtotal_brl
--   freight_total_brl
--   order_item_total_brl
--   payment_record_count
--   payment_total_brl
--   payment_item_difference_brl
--
-- Non-additive attributes/measures:
--   delivery durations
--   review score
--   flags and counts used as diagnostics

with orders as (

    select *
    from {{ ref('int_orders_enriched') }}

),

final as (

    select
        order_id,
        customer_id,
        customer_unique_id,

        order_status,
        order_purchase_ts,
        order_purchase_date,
        order_approved_ts,
        order_delivered_carrier_ts,
        order_delivered_customer_ts,
        order_estimated_delivery_ts,

        is_delivered,
        delivery_duration_days,
        delivery_delay_days,
        is_late_delivery,

        has_order_items,
        item_count,
        distinct_product_count,
        item_subtotal_brl,
        freight_total_brl,
        order_item_total_brl,

        has_payment,
        payment_record_count,
        payment_method_count,
        maximum_installments,
        payment_total_brl,
        payment_item_difference_brl,

        has_review,
        review_id,
        review_score,
        review_created_ts,
        review_answered_ts,
        review_record_count
    from orders

)

select *
from final