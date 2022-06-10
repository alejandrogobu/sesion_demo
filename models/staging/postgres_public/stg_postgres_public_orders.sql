WITH src_orders AS (
    SELECT * 
    FROM {{ source('postgres_public','orders') }}
),

renamed_casted AS (
    SELECT
        order_id 
        , user_id 
        , CASE 
            WHEN promo_id = '' THEN null
            WHEN promo_id <> '' THEN {{ dbt_utils.surrogate_key('promo_id') }} 
            ELSE null 
          END AS promo_id
        , address_id
        , created_at AS created_at_utc
        , order_cost AS item_order_cost_usd
        , shipping_cost AS shipping_cost_usd
        , order_total AS total_order_cost_usd
        , tracking_id
        , shipping_service
        , estimated_delivery_at AS estimated_delivery_at_utc
        , delivered_at AS delivered_at_utc
        , status AS status_order
        , _fivetran_synced as date_load
    FROM src_orders
    )

SELECT * FROM renamed_casted