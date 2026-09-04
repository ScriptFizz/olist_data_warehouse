with staging as (

    select
        count(*) as row_count,
        count(distinct customer_id) as distinct_key_count
    from {{ ref('stg_customers') }}

),

dimension as (

    select
        count(*) as row_count,
        count(distinct customer_id) as distinct_key_count
    from {{ ref('dim_customers') }}

)

select *
from staging
cross join dimension
where staging.row_count <> dimension.row_count
   or dimension.row_count <> dimension.distinct_key_count