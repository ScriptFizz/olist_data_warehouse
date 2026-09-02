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
        estimated_delivery_ts as order_estimated_delivery_ts,
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source

)

select *
from renamed