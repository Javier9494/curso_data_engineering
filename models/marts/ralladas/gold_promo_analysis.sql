{{ config(materialized='table') }}

WITH promo_data AS (
    SELECT 
        p.PROMO_ID,
        p.PROMO_NAME,
        p.DISCOUNT,
        COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS_WITH_PROMO,
        SUM(o.ORDER_TOTAL - o.ORDER_COST) AS TOTAL_DISCOUNT_VALUE,
        SUM(o.ORDER_TOTAL) AS REVENUE_WITH_PROMO
    FROM {{ ref('stg_sql_server_dbo__promos') }} p
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders') }} o
        ON p.PROMO_ID = o.PROMO_ID
    WHERE o._FIVETRAN_DELETED = FALSE
    GROUP BY p.PROMO_ID, p.PROMO_NAME, p.DISCOUNT
)
SELECT 
    PROMO_ID,
    PROMO_NAME,
    DISCOUNT,
    TOTAL_ORDERS_WITH_PROMO,
    TOTAL_DISCOUNT_VALUE,
    REVENUE_WITH_PROMO
FROM promo_data
