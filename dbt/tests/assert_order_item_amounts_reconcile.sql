select 
    order_id,
    item_subtotal_brl,
    freight_total_brl,
    order_item_total_brl
from {{ ref('int_order_item_metrics') }}
where abs(
    order_item_total_brl
    - item_subtotal_brl
    - freight_total_brl
) > 0.000001