select
    order_id,
    order_status,
    is_delivered,
    order_delivered_customer_ts,
    order_estimated_delivery_ts,
    delivery_duration_days,
    delivery_delay_days,
    is_late_delivery
from {{ ref('int_orders_enriched') }}
where is_delivered <> (order_status = 'delivered')
   or (
       order_status <> 'delivered'
       and (
           delivery_duration_days is not null
           or delivery_delay_days is not null
           or is_late_delivery is not null
       )
   )
   or (
       order_status = 'delivered'
       and order_delivered_customer_ts is not null
       and (
           delivery_duration_days is null
           or delivery_delay_days is null
           or is_late_delivery is null
       )
   )
   or (
       delivery_delay_days > 0
       and is_late_delivery is not true
   )
   or (
       delivery_delay_days <= 0
       and is_late_delivery is true
   )