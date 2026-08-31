with source as (

    select *
    from {{ source('olist_raw', 'sellers') }}

),

renamed as (

    select
        seller_id,
        zipcode as seller_zip_code_prefix,
        city as seller_city,
        state as seller_state_code
    from source

)

select *
from renamed