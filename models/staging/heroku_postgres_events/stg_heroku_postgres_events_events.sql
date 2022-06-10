WITH src_events AS (
    SELECT * 
    FROM {{ source('heroku_postgres_events', 'events') }}
    ),

renamed_casted AS (
    SELECT
        event_id
        , session_id
        , user_id
        , page_url
        , created_at AS created_at_utc
        , event_type
        , order_id
        , product_id
        , _fivetran_synced AS date_load
    FROM src_events
    )

SELECT * FROM renamed_casted