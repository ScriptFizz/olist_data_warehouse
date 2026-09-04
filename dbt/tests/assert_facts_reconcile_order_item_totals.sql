with item_totals as (

    select
        order_id,
        count(*) as item_count,
        count(distinct product_id) as distinct_product_count,
        sum(item_price_brl) as item_subtotal_brl,
        sum(freight_value_brl) as freight_total_brl,
        sum(item_total_brl) as order_item_total_brl
    from {{ ref('fct_order_items') }}
    group by order_id

),

orders as (

    select *
    from {{ ref('fct_orders') }}

)

select
    orders.order_id
from orders
left join item_totals
    on orders.order_id = item_totals.order_id
where orders.item_count
        <> coalesce(item_totals.item_count, 0)
   or orders.distinct_product_count
        <> coalesce(item_totals.distinct_product_count, 0)
   or abs(
       orders.item_subtotal_brl
       - coalesce(item_totals.item_subtotal_brl, 0.0)
   ) > 0.000001
   or abs(
       orders.freight_total_brl
       - coalesce(item_totals.freight_total_brl, 0.0)
   ) > 0.000001
   or abs(
       orders.order_item_total_brl
       - coalesce(item_totals.order_item_total_brl, 0.0)
   ) > 0.000001