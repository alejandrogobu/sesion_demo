WITH src_products AS (
    SELECT * 
    FROM {{ source('postgres_public', 'products') }}
    ),

renamed_casted AS (
    SELECT
        product_id AS product_id
        , name AS product_name
        , price AS unit_price_usd
        , inventory AS inventory
        , _fivetran_synced AS date_load
        FROM src_products
    )

SELECT * FROM renamed_casted