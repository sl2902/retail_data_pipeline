

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