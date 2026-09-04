-- We  do not assert that payment total always equals item total. 
-- Real commerce datasets can contain cancellations, adjustments, vouchers, etc etc. 
-- Instead, we assert that the reported difference is calculated correctly.

select
    order_id,
    item_subtotal_brl,
    freight_total_brl,
    order_item_total_brl,
    payment_total_brl,
    payment_item_difference_brl
from {{ ref('int_orders_enriched') }}
where abs(
    order_item_total_brl
    - item_subtotal_brl
    - freight_total_brl
) > 0.000001
or abs(
    payment_item_difference_brl
    - payment_total_brl
    + order_item_total_brl
) > 0.000001