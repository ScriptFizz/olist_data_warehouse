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
        cast(_batch_id as {{ dbt.type_string() }}) as ingestion_batch_id,
        cast(_ingested_at as {{ dbt.type_timestamp() }}) as source_ingested_at,
        cast(_record_hash as {{ dbt.type_string() }}) as source_record_hash
    from source

)

select *
from renamed
