WITH stg_postgres_public_addresses AS (
    SELECT * 
    FROM {{ ref ('stg_postgres_public_addresses') }}
    ),

renamed_casted AS (
    SELECT
        address_id
        , address
        , zipcode
        , state
        , country
        , date_load
    FROM stg_postgres_public_addresses
    )

SELECT * FROM renamed_casted