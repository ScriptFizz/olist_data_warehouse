with source as (

    select *
    from {{ source('olist_raw', 'payments') }}

),

renamed as (

    select
        order_id,
        sequential as payment_sequential,
        type as payment_type,
        installments as payment_installments,
        value as payment_value_brl,
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source

)

select *
from renamed