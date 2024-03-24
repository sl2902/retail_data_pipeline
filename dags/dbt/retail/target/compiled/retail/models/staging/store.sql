

with base as (
    SELECT
        store_id,
        location,
        size,
        manager
    FROM
       `hive-413217`.`retail_2`.`store`
)

SELECT
    *
FROM
    base