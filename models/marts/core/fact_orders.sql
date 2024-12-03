-- models/facts/fact_orders.sql
WITH base AS (
    SELECT
        order_id,
        user_id,
        address_id,
        promo_id,
        shipping_service,
        shipping_cost,
        order_cost,
        order_total,
        created_at,
        estimated_delivery_at,
        delivered_at,
        status_orders,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED_UTC
    FROM {{ ref('stg_sql_server_dbo__orders') }}
)
SELECT
    order_id,
    user_id,
    address_id,
    promo_id,
    shipping_service,
    shipping_cost,
    order_cost,
    order_total,
    created_at,
    estimated_delivery_at,
    delivered_at,
    status_orders,
    _FIVETRAN_DELETED,
    _FIVETRAN_SYNCED_UTC
FROM base
