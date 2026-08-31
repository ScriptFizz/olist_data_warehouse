with payment as (
    select *
    from {{ ref('stg_payments') }}
),

aggregated as (
    select
        order_id,
        count(*) as payment_record_count,
        count(distinct payment_type) as payment_method_count,
        max(payment_installments) as maximum_installments,
        sum(payment_value_brl) as payment_total_brl
    from payment
    group by order_id
)

select * 
from aggregated