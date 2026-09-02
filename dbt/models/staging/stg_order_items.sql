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
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source

)

select *
from renamed