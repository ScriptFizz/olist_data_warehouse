with source as (

    select *
    from {{ source('olist_raw', 'translation') }}

),

renamed as (

    select
        name_brz as product_category_name_portuguese,
        name_eng as product_category_name_english
    from source

)

select *
from renamed