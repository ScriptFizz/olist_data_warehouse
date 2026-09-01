with staging as (

    select
        count(*) as row_count,
        count(distinct product_id) as distinct_key_count
    from {{ ref('stg_products') }}

),

dimension as (

    select
        count(*) as row_count,
        count(distinct product_id) as distinct_key_count
    from {{ ref('dim_products') }}

)

select *
from staging
cross join dimension
where staging.row_count <> dimension.row_count
   or dimension.row_count <> dimension.distinct_key_count