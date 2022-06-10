WITH fact_events AS (
    SELECT * 
    FROM {{ ref('fct_events') }}
    ),

int_session_events_users AS (
    SELECT 
        session_id
        ,created_at_utc
        ,user_id
        ,product_id
        ,{{column_values_to_metrics(ref('fct_events'),'event_type')}}
    FROM fact_events

    {{ dbt_utils.group_by(4) }}
    )


SELECT * FROM int_session_events_users