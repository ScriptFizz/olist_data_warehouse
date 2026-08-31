with source as (

    select *
    from {{ source('olist_raw', 'order_items') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_limit_ts,
        price as item_price_brl,
        freight_value as freight_value_brl
    from source

)

select *
from renamed