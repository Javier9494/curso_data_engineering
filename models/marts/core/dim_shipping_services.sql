-- models/dimensions/dim_shipping_services.sql
WITH base AS (
    SELECT DISTINCT
        shipping_service
    FROM {{ ref('stg_sql_server_dbo__orders') }}
)
SELECT
    shipping_service
FROM base
