-- models/dimensions/dim_event_types.sql
WITH base AS (
    SELECT DISTINCT
        event_type
    FROM {{ ref('stg_sql_server_dbo__events') }}
)
SELECT
    event_type
FROM base
