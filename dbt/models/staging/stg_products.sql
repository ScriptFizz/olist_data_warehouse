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
        _batch_id as ingestion_batch_id,
        _ingested_at as source_ingested_at,
        _record_hash as source_record_hash
    from source

)

select *
from renamed