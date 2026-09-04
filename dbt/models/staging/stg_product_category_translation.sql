with source as (

    select *
    from {{ source('olist_raw', 'translation') }}

),

renamed as (

    select
        name_brz as product_category_name_portuguese,
        name_eng as product_category_name_english,
        cast(_batch_id as {{ dbt.type_string() }}) as ingestion_batch_id,
        cast(_ingested_at as {{ dbt.type_timestamp() }}) as source_ingested_at,
        cast(_record_hash as {{ dbt.type_string() }}) as source_record_hash
    from source

)

select *
from renamed
