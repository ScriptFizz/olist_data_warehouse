-- Grain: exactly one row per order_id and order_item_id.
--
-- item_price_brl, freight_value_brl, and item_total_brl are additive
-- at this fact's declared grain.

with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        customer_unique_id,
        order_status,
        order_purchase_ts,
        order_purchase_date,
        is_delivered,
        order_updated_at,
        customer_updated_at
    from {{ ref('int_orders_enriched') }}

),

final as (

    select
        order_items.order_id,
        order_items.order_item_id,
        order_items.product_id,
        order_items.seller_id,

        orders.customer_id,
        orders.customer_unique_id,
        orders.order_status,
        orders.order_purchase_ts,
        orders.order_purchase_date,
        orders.is_delivered,

        order_items.shipping_limit_ts,

        order_items.item_price_brl,
        order_items.freight_value_brl,
        (
            order_items.item_price_brl
            + order_items.freight_value_brl
        ) as item_total_brl,
        {{ greatest_timestamp([
            'order_items.source_ingested_at',
            'orders.order_updated_at',
            'orders.customer_updated_at'
        ]) }} as warehouse_updated_at

    from order_items
    inner join orders
        on order_items.order_id = orders.order_id

)

select final.*
from final

{% if is_incremental() %}

left join {{ this }} as existing
    on final.order_id = existing.order_id
    and final.order_item_id = existing.order_item_id

where existing.order_id is null
    or final.warehouse_updated_at > existing.warehouse_updated_at

{% endif %}