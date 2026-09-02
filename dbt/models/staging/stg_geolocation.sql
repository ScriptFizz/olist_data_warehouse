with source as (

    select *
    from {{ source('olist_raw', 'geolocation') }}

),

renamed as (

    select
        zipcode as geo_zip_code_prefix,
        lat as latitude,
        lng as longitude,
        city as geo_city,
        state as geo_state_code,
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source

)

select *
from renamed