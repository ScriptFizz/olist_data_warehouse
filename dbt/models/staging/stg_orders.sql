with source as (

    select *
    from {{ source('olist_raw', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,
        purchase_ts as order_purchase_ts,
        approval_ts as order_approved_ts,
        delivery_carrier_ts as order_delivered_carrier_ts,
        delivery_customer_ts as order_delivered_customer_ts,
        estimated_delivery_ts as order_estimated_delivery_ts
    from source

)

select *
from renamed