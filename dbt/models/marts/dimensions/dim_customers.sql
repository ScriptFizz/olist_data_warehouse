-- Grain: one row per customer_id.

with customers as (

    select *
    from {{ ref('stg_customers') }}

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
        customers.customer_id,
        customers.customer_unique_id,
        customers.customer_zip_code_prefix,
        customers.customer_city,
        customers.customer_state_code,

        states.state_name as customer_state_name,
        states.state_iso_code as customer_state_iso_code,

        geolocation.geo_city as geolocation_city,
        geolocation.geo_state_code as geolocation_state_code,
        geolocation.latitude,
        geolocation.longitude,

        geolocation.geo_zip_code_prefix is not null
            as has_geolocation_match

    from customers
    left join states
        on customers.customer_state_code = states.state_code
    left join geolocation
        on customers.customer_zip_code_prefix
            = geolocation.geo_zip_code_prefix

)

select *
from final