

with base as (
    SELECT
        transaction_id,
        product_id,
        timestamp,
        quantity,
        unit_price,
        store_id
    from
        `hive-413217`.`retail_2`.`transactions`
)

SELECT
    *
FROM
    base