with source as (
    select * 
    from {{ source('olist_raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        customer_uid as customer_unique_id,
        zipcode,
        city,
        state as state_code
    from source
)

select * 
from renamed