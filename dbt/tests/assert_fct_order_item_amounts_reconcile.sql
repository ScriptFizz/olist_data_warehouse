select
    order_id,
    order_item_id,
    item_price_brl,
    freight_value_brl,
    item_total_brl
from {{ ref('fct_order_items') }}
where abs(
    item_total_brl
    - item_price_brl
    - freight_value_brl
) > 0.000001