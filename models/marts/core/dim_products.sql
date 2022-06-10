WITH stg_postgres_public_products AS (
    SELECT * 
    FROM {{ ref ('stg_postgres_public_products') }}
    ),

renamed_casted AS (
    SELECT
        product_id 
        , product_name
        , unit_price_usd
        , inventory
        , date_load
    FROM stg_postgres_public_products
    )

SELECT * FROM renamed_casted