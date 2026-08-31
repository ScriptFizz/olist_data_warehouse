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
        state as customer_state_code
    from source
)

select * 
from renamed