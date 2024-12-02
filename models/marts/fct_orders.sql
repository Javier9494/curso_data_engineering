{{ config(materialized='incremental') }}

 SELECT 
        order_id,
        shipping_service,
        shipping_cost,
        address_id,
        created_at,
        promo_id,
        promo_name,
        estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        delivered_at,
        status_orders,
        _FIVETRAN_DELETED,
       _FIVETRAN_SYNCED_UTC
     FROM {{ ref('stg_sql_server_dbo__orders') }} 