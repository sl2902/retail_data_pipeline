{{
config(
    materialized = 'materialized_view',
    on_configuration_change = 'apply',
    enable_refresh = True,
    refresh_interval_minutes = 240
)
}}

with cte as (
  select
    transaction_id,
    quantity,
    unit_price,
    timestamp,
    name,
    category,
    location,
    size,
    manager
  from
   {{ ref('transactions') }} t join {{ ref('product') }} p on t.product_id = p.product_id
   join {{ ref('store') }} s on t.store_id = s.store_id
)

select
 *
from
 cte