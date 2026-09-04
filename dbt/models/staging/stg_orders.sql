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
        cast(_batch_id as {{ dbt.type_string() }}) as ingestion_batch_id,
        cast(_ingested_at as {{ dbt.type_timestamp() }}) as source_ingested_at,
        cast(_record_hash as {{ dbt.type_string() }}) as source_record_hash
    from source

)

select *
from renamed
