WITH stg_google_sheets_budget AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets_budget') }}
    ),

renamed_casted AS (
    SELECT
        product_id
        , month
        , quantity
        , date_load
    FROM stg_google_sheets_budget
    )

SELECT * FROM renamed_casted