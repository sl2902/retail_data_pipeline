{{ 
    config(materialized='table') 
}}

with base as (
    SELECT
        product_id,
        name,
        category,
        base_price,
        supplier_id
    FROM
       {{ source('staging', 'product') }}
)

SELECT
    *
FROM
    base