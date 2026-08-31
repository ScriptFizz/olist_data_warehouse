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
        state as geo_state_code
    from source

)

select *
from renamed