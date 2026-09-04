with staging as (

    select
        count(*) as row_count,
        count(distinct order_id) as distinct_order_count
    from {{ ref('stg_orders') }}

),

canonical as (

    select
        count(*) as row_count,
        count(distinct order_id) as distinct_order_count
    from {{ ref('int_orders_enriched') }}

)

select
    staging.row_count as staging_row_count,
    canonical.row_count as canonical_row_count
from staging
cross join canonical
where staging.row_count <> canonical.row_count
   or canonical.row_count <> canonical.distinct_order_count