-- Grain: one row per product_id.

with products as (

    select *
    from {{ ref('int_product_categories') }}

),

final as (

    select
        product_id,
        product_category_name,
        product_category_name_portuguese,
        product_category_name_english,
        has_english_translation,
        product_name_length,
        product_description_length,
        product_photos_quantity,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from products

)

select *
from final