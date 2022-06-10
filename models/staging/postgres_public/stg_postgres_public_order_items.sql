WITH src_order_items AS (
    SELECT * 
    FROM {{ source('postgres_public', 'order_items') }}
    ),

renamed_casted AS (
    SELECT
        order_id
        , product_id
        , quantity
        , _fivetran_synced AS date_load
    FROM src_order_items
    )

SELECT * FROM renamed_casted