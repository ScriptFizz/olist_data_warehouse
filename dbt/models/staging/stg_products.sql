with source as (

    select *
    from {{ source('olist_raw', 'products') }}

),

renamed as (

    select
        product_id,
        name as product_category_name_portuguese,
        name_length as product_name_length,
        description_length as product_description_length,
        photos_qty as product_photos_quantity,
        weight_g as product_weight_g,
        length_cm as product_length_cm,
        height_cm as product_height_cm,
        width_cm as product_width_cm,
        cast(_batch_id as {{ dbt.type_string() }}) as ingestion_batch_id,
        cast(_ingested_at as {{ dbt.type_timestamp() }}) as source_ingested_at,
        cast(_record_hash as {{ dbt.type_string() }}) as source_record_hash
    from source

)

select *
from renamed
