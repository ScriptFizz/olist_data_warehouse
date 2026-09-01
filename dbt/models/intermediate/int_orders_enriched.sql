-- Grain: exactly one row per order_id.

with orders as (

    select *
    from {{ ref('stg_orders') }}

),

customers as (

    select *
    from {{ ref('stg_customers') }}

),

item_metrics as (

    select *
    from {{ ref('int_order_item_metrics') }}

),

payment_metrics as (

    select *
    from {{ ref('int_order_payment_metrics') }}

),

reviews as (

    select *
    from {{ ref('int_order_reviews_deduplicated') }}

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        customers.customer_unique_id,

        orders.order_status,
        orders.order_purchase_ts,
        cast(orders.order_purchase_ts as date) as order_purchase_date,
        orders.order_approved_ts,
        orders.order_delivered_carrier_ts,
        orders.order_delivered_customer_ts,
        orders.order_estimated_delivery_ts,

        orders.order_status = 'delivered' as is_delivered,

        case
            when orders.order_status = 'delivered'
                and orders.order_delivered_customer_ts is not null
            then {{ timestamp_diff_days(
                'orders.order_delivered_customer_ts',
                'orders.order_purchase_ts'
            ) }}
        end as delivery_duration_days,

        case
            when orders.order_status = 'delivered'
                and orders.order_delivered_customer_ts is not null
            then {{ timestamp_diff_days(
                'orders.order_delivered_customer_ts',
                'orders.order_estimated_delivery_ts'
            ) }}
        end as delivery_delay_days,

        case
            when orders.order_status = 'delivered'
                and orders.order_delivered_customer_ts is not null
            then (
                orders.order_delivered_customer_ts
                > orders.order_estimated_delivery_ts
            )
        end as is_late_delivery,

        item_metrics.order_id is not null as has_order_items,
        coalesce(item_metrics.item_count, 0) as item_count,
        coalesce(
            item_metrics.distinct_product_count,
            0
        ) as distinct_product_count,
        coalesce(
            item_metrics.item_subtotal_brl,
            0.0
        ) as item_subtotal_brl,
        coalesce(
            item_metrics.freight_total_brl,
            0.0
        ) as freight_total_brl,
        coalesce(
            item_metrics.order_item_total_brl,
            0.0
        ) as order_item_total_brl,

        payment_metrics.order_id is not null as has_payment,
        coalesce(
            payment_metrics.payment_record_count,
            0
        ) as payment_record_count,
        coalesce(
            payment_metrics.payment_method_count,
            0
        ) as payment_method_count,
        payment_metrics.maximum_installments,
        coalesce(
            payment_metrics.payment_total_brl,
            0.0
        ) as payment_total_brl,

        coalesce(
            payment_metrics.payment_total_brl,
            0.0
        ) - coalesce(
            item_metrics.order_item_total_brl,
            0.0
        ) as payment_item_difference_brl,

        reviews.order_id is not null as has_review,
        reviews.review_id,
        reviews.review_score,
        reviews.review_title,
        reviews.review_message,
        reviews.review_created_ts,
        reviews.review_answered_ts,
        coalesce(
            reviews.review_record_count,
            0
        ) as review_record_count

    from orders
    left join customers
        on orders.customer_id = customers.customer_id
    left join item_metrics
        on orders.order_id = item_metrics.order_id
    left join payment_metrics
        on orders.order_id = payment_metrics.order_id
    left join reviews
        on orders.order_id = reviews.order_id

)

select *
from final