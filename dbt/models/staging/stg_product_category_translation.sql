with source as (

    select *
    from {{ source('olist_raw', 'translation') }}

),

renamed as (

    select
        name_brz as product_category_name_portuguese,
        name_eng as product_category_name_english,
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source

)

select *
from renamed