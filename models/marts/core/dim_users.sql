WITH stg_postgres_public_users AS (
    SELECT * 
    FROM {{ ref('stg_postgres_public_users') }}
    ),

renamed_casted AS (
    SELECT
        user_id
        , first_name
        , last_name
        , email
        , phone_number
        , created_at_utc
        , updated_at_utc
        , address_id
        , date_load
    FROM stg_postgres_public_users
    )

SELECT * FROM renamed_casted