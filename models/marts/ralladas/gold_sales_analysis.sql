{{ config(materialized='table') }}

WITH orders AS (
    SELECT 
        o.ORDER_ID,
        o.USER_ID,
        o.PROMO_ID,
        o.ORDER_TOTAL,
        o.ORDER_COST,
        o.SHIPPING_COST,
        o.ESTIMATED_DELIVERY_AT,
        o.DELIVERED_AT,
        o.STATUS_ORDERS,
        o.CREATED_AT,
        p.PROMO_NAME,
        p.DISCOUNT
    FROM {{ ref('stg_sql_server_dbo__orders') }} o
    LEFT JOIN {{ ref('stg_sql_server_dbo__promos') }} p
        ON o.PROMO_ID = p.PROMO_ID
    WHERE o._FIVETRAN_DELETED = FALSE
),
order_items AS (
    SELECT 
        oi.ORDER_ID,
        oi.PRODUCT_ID,
        SUM(oi.QUANTITY) AS QUANTITY
    FROM {{ ref('stg_sql_server_dbo__order_items') }} oi
    WHERE oi._FIVETRAN_DELETED = FALSE
    GROUP BY oi.ORDER_ID, oi.PRODUCT_ID
),
orders_with_date AS (
    SELECT 
        o.ORDER_ID,
        o.USER_ID,
        oi.PRODUCT_ID,
        o.PROMO_ID,
        o.PROMO_NAME,
        o.ORDER_TOTAL,
        o.ORDER_COST,
        o.DISCOUNT,
        oi.QUANTITY,
        o.SHIPPING_COST,
        o.DELIVERED_AT,
        o.ESTIMATED_DELIVERY_AT,
        o.STATUS_ORDERS,
        o.CREATED_AT,
        d.DAY_OF_WEEK_NAME,
        d.MONTH_NAME,
        d.YEAR_NUMBER,
        d.PRIOR_YEAR_DATE_DAY
    FROM orders o
    LEFT JOIN order_items oi
        ON o.ORDER_ID = oi.ORDER_ID
    LEFT JOIN {{ ref('dim_date') }} d
        ON CAST(o.CREATED_AT AS DATE) = d.DATE_DAY
)
SELECT 
    ORDER_ID,
    USER_ID,
    PRODUCT_ID,
    PROMO_ID,
    PROMO_NAME,
    ORDER_TOTAL,
    ORDER_COST,
    DISCOUNT,
    QUANTITY,
    SHIPPING_COST,
    DELIVERED_AT,
    ESTIMATED_DELIVERY_AT,
    STATUS_ORDERS,
    CREATED_AT,
    DAY_OF_WEEK_NAME,
    MONTH_NAME,
    YEAR_NUMBER,
    PRIOR_YEAR_DATE_DAY
FROM orders_with_date
