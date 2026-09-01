with canonical as (

    select
        count(*) as row_count,
        count(distinct order_id) as distinct_key_count
    from {{ ref('int_orders_enriched') }}

),

fact as (

    select
        count(*) as row_count,
        count(distinct order_id) as distinct_key_count
    from {{ ref('fct_orders') }}

)

select *
from canonical
cross join fact
where canonical.row_count <> fact.row_count
   or fact.row_count <> fact.distinct_key_count