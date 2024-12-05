{{ config(materialized='table') }}

SELECT 
    o.ORDER_ID,
    o.SHIPPING_SERVICE,
    o.SHIPPING_COST,
    o.ESTIMATED_DELIVERY_AT,
    o.DELIVERED_AT,
    DATEDIFF('day', o.CREATED_AT, o.DELIVERED_AT) AS DELIVERY_TIME,
    o.STATUS_ORDERS
FROM {{ ref('stg_sql_server_dbo__orders') }} o
WHERE o._FIVETRAN_DELETED = FALSE
