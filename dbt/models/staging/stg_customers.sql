with source as (
    select * 
    from {{ source('olist_raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        customer_uid as customer_unique_id,
        zipcode as customer_zip_code_prefix,
        city as customer_city,
        state as customer_state_code,
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source
)

select * 
from renamed