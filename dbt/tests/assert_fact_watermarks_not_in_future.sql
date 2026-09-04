select order_id 
from {{ ref('fct_orders') }}
where warehouse_updated_at > {{ dbt.current_timestamp() }}

union all

select order_id
from {{ ref('fct_order_items') }}
where warehouse_updated_at > {{ dbt.current_timestamp() }}