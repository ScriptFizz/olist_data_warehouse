select
    order_id,
    has_order_items,
    item_count,
    has_payment,
    payment_record_count,
    has_review,
    review_record_count
from {{ ref('int_orders_enriched') }}
where has_order_items <> (item_count > 0)
   or has_payment <> (payment_record_count > 0)
   or has_review <> (review_record_count > 0)