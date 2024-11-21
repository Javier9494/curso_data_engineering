{{ config(materialized='table') }}

 SELECT 
        order_id,
        CASE 
        WHEN shipping_service IS NULL OR shipping_service = '' THEN 'en proceso' 
        ELSE shipping_service 
        END AS shipping_service,
        shipping_cost,
        address_id,
        created_at,
        CASE 
        WHEN promo_id IS NULL OR promo_id = '' THEN 'no promo' 
        ELSE promo_id 
        END AS promo_id,
        estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        delivered_at,
        tracking_id,
        status,
        _fivetran_synced
    FROM {{ source('sql_server_dbo', 'orders') }} 