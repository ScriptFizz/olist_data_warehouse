-- Grain: one row per order with at least one order item.

with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

aggregated as (

    select
        order_id,
        count(*) as item_count,
        count(distinct product_id) as distinct_product_count,
        sum(item_price_brl) as item_subtotal_brl,
        sum(freight_value_brl) as freight_total_brl,
        sum(item_price_brl + freight_value_brl) as order_item_total_brl,
        max(source_ingested_at) as item_metrics_updated_at
    from order_items
    group by order_id

)

select *
from aggregated