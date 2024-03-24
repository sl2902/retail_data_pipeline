
  
    

    create or replace table `hive-413217`.`dbt_retail`.`product`
      
    
    

    OPTIONS()
    as (
      

with base as (
    SELECT
        product_id,
        name,
        category,
        base_price,
        supplier_id
    FROM
       `hive-413217`.`retail_2`.`product`
)

SELECT
    *
FROM
    base
    );
  