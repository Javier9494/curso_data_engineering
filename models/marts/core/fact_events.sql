-- models/facts/fact_events.sql
WITH base AS (
    SELECT
        event_id,
        user_id,
        product_id,
        session_id,
        order_id,
        event_type,
        created_at,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED_UTC
    FROM {{ ref('stg_sql_server_dbo__events') }}
)
SELECT
    event_id,
    user_id,
    product_id,
    session_id,
    order_id,
    event_type,
    created_at,
    _FIVETRAN_DELETED,
    _FIVETRAN_SYNCED_UTC
FROM base
