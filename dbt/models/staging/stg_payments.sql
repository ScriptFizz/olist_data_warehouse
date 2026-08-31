with source as (

    select *
    from {{ source('olist_raw', 'payments') }}

),

renamed as (

    select
        order_id,
        sequential as payment_sequential,
        type as payment_type,
        installments as payment_installments,
        value as payment_value_brl
    from source

)

select *
from renamed