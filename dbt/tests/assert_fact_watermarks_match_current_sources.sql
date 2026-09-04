with order_mismatches as (

    select
        fact.order_id
    from {{ ref('fct_orders') }} as fact
    inner join {{ ref('int_orders_enriched') }} as source
        on fact.order_id = source.order_id
    where fact.warehouse_updated_at <> source.warehouse_updated_at

),

item_mismatches as (

    select
        fact.order_id
    from {{ ref('fct_order_items') }} as fact
    inner join {{ ref('stg_order_items') }} as item
        on fact.order_id = item.order_id
        and fact.order_item_id = item.order_item_id
    inner join {{ ref('int_orders_enriched') }} as orders
        on fact.order_id = orders.order_id
    where fact.warehouse_updated_at <> {{ greatest_timestamp([
        'item.source_ingested_at',
        'orders.order_updated_at',
        'orders.customer_updated_at'
    ]) }}

)

select order_id
from order_mismatches

union all

select order_id
from item_mismatches