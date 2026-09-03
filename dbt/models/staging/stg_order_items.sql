with source as (

    select *
    from {{ source('olist_raw', 'order_items') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_limit_ts,
        price as item_price_brl,
        freight_value as freight_value_brl,
        cast(_batch_id as {{ dbt.type_string() }}) as ingestion_batch_id,
        cast(_ingested_at as {{ dbt.type_timestamp() }}) as source_ingested_at,
        cast(_record_hash as {{ dbt.type_string() }}) as source_record_hash
    from source

)

select *
from renamed
