with staging as (

    select count(*) as row_count
    from {{ ref('stg_order_items') }}

),

fact as (

    select count(*) as row_count
    from {{ ref('fct_order_items') }}

)

select *
from staging
cross join fact
where staging.row_count <> fact.row_count