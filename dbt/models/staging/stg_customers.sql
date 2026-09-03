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
        cast(_batch_id as {{ dbt.type_string() }}) as ingestion_batch_id,
        cast(_ingested_at as {{ dbt.type_timestamp() }}) as source_ingested_at,
        cast(_record_hash as {{ dbt.type_string() }}) as source_record_hash
    from source
)

select * 
from renamed
