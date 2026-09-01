with staging as (

    select
        count(*) as row_count,
        count(distinct seller_id) as distinct_key_count
    from {{ ref('stg_sellers') }}

),

dimension as (

    select
        count(*) as row_count,
        count(distinct seller_id) as distinct_key_count
    from {{ ref('dim_sellers') }}

)

select *
from staging
cross join dimension
where staging.row_count <> dimension.row_count
   or dimension.row_count <> dimension.distinct_key_count