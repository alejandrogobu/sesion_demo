WITH stg_postgres_public_promos AS (
    SELECT * 
    FROM {{ ref ('stg_postgres_public_promos') }}
    ),

renamed_casted AS (
    SELECT
        promo_id
        , name_promo
        , total_discount_usd
        , status_promo
        , date_load
    FROM stg_postgres_public_promos
    )

SELECT * FROM renamed_casted