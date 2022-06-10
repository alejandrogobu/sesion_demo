WITH stg_heroku_postgres_events_events AS (
    SELECT * 
    FROM {{ ref('stg_heroku_postgres_events_events') }}
    ),

renamed_casted AS (
    SELECT
        event_id
        , session_id
        , user_id
        , page_url
        , created_at_utc
        , event_type
        , order_id
        , product_id
        , date_load
    FROM stg_heroku_postgres_events_events
    )

SELECT * FROM renamed_casted