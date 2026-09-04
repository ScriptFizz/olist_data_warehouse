-- Grain: one row per seller_id.

with sellers as (

    select *
    from {{ ref('stg_sellers') }}

),

geolocation as (

    select *
    from {{ ref('int_geolocation_zip_codes') }}

),

states as (

    select *
    from {{ ref('brazil_states') }}

),

final as (

    select
        sellers.seller_id,
        sellers.seller_zip_code_prefix,
        sellers.seller_city,
        sellers.seller_state_code,

        states.state_name as seller_state_name,
        states.state_iso_code as seller_state_iso_code,

        geolocation.geo_city as geolocation_city,
        geolocation.geo_state_code as geolocation_state_code,
        geolocation.latitude,
        geolocation.longitude,

        geolocation.geo_zip_code_prefix is not null
            as has_geolocation_match

    from sellers
    left join states
        on sellers.seller_state_code = states.state_code
    left join geolocation
        on sellers.seller_zip_code_prefix
            = geolocation.geo_zip_code_prefix

)

select *
from final