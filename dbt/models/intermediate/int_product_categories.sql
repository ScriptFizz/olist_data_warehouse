-- Grain: one row per product.

with products as (

    select *
    from {{ ref('stg_products') }}

),

translations as (

    select *
    from {{ ref('stg_product_category_translation') }}

),

enriched as (

    select
        products.product_id,
        products.product_category_name_portuguese,
        translations.product_category_name_english,

        coalesce(
            translations.product_category_name_english,
            products.product_category_name_portuguese,
            'unknown'
        ) as product_category_name,

        translations.product_category_name_english is not null
            as has_english_translation,

        products.product_name_length,
        products.product_description_length,
        products.product_photos_quantity,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm
    from products
    left join translations
        on products.product_category_name_portuguese
            = translations.product_category_name_portuguese

)

select *
from enriched