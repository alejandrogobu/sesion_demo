{% set event_types = dbt_utils.get_column_values(ref('fct_events'),'event_type') %}
WITH fct_events AS (
    SELECT * 
    FROM {{ ref('fct_events') }}
    ),


int_session_events_users AS (
    SELECT * 
    FROM {{ ref('int_session_events_users') }}
    ),
    

dim_users AS (
    SELECT * 
    FROM {{ ref('dim_users') }}
    ),


int_session_events_agg AS (
    SELECT 
        session_id,
        user_id,
        min(created_at_utc) as first_event_time_utc,
        max (created_at_utc) as last_event_time_utc,
        {%- for event_type in event_types %}
        sum({{event_type}}) as {{event_type}}
        {%- if not loop.last %},{% endif -%}
        {% endfor %}
    FROM int_session_events_users

    {{ dbt_utils.group_by(2) }}
    ),


fct_user_sessions AS (
    SELECT
        i.session_id,
        i.user_id,
        u.first_name,
        u.email,
        i.first_event_time_utc,
        i.last_event_time_utc,
        DATEDIFF(MINUTE, cast(i.first_event_time_utc as timestamp), cast(i.last_event_time_utc as timestamp)) AS session_length_minutes,
        {%- for event_type in event_types %}
        i.{{event_type}}
        {%- if not loop.last %},{% endif -%}
        {% endfor %}

    FROM int_session_events_agg I
    LEFT JOIN dim_users U ON i.user_id = u.user_id
    )

SELECT * FROM fct_user_sessions