{{ 
    config(materialized='table') 
}}

with base as (
    SELECT
        transaction_id,
        product_id,
        timestamp,
        quantity,
        unit_price,
        store_id
    from
        {{ source('staging', 'transactions')}}
)

SELECT
    *
FROM
    base