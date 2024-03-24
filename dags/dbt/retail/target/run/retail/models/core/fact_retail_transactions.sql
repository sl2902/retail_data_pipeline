
  
    

    create or replace table `hive-413217`.`dbt_retail`.`fact_retail_transactions`
      
    
    

    OPTIONS()
    as (
      

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
   `hive-413217`.`dbt_retail`.`transactions` t join `hive-413217`.`dbt_retail`.`product` p on t.product_id = p.product_id
   join `hive-413217`.`dbt_retail`.`store` s on t.store_id = s.store_id
)

select
 *
from
 cte
    );
  