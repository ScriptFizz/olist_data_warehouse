select 
    order_id,
    review_record_count
from {{ ref('int_order_reviews_deduplicated') }}
where review_record_count < 1