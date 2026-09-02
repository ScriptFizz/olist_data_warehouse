with source as (

    select *
    from {{ source('olist_raw', 'sellers') }}

),

renamed as (

    select
        seller_id,
        zipcode as seller_zip_code_prefix,
        city as seller_city,
        state as seller_state_code,
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source

)

select *
from renamed