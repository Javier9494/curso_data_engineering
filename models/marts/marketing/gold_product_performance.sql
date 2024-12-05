{{ config(materialized='table') }}

WITH sales_data AS (
    SELECT 
        oi.PRODUCT_ID,
        SUM(oi.QUANTITY) AS QUANTITY_SOLD,
        SUM(oi.QUANTITY * p.PRICE) AS REVENUE
    FROM {{ ref('stg_sql_server_dbo__order_items') }} oi
    LEFT JOIN {{ ref('stg_sql_server_dbo__products') }} p
        ON oi.PRODUCT_ID = p.PRODUCT_ID
    WHERE oi._FIVETRAN_DELETED = FALSE
    GROUP BY oi.PRODUCT_ID
),
promo_usage AS (
    SELECT 
        oi.PRODUCT_ID,
        COUNT(DISTINCT o.ORDER_ID) AS PROMO_USAGE
    FROM {{ ref('stg_sql_server_dbo__order_items') }} oi
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders') }} o
        ON oi.ORDER_ID = o.ORDER_ID
    WHERE o.PROMO_ID IS NOT NULL
    GROUP BY oi.PRODUCT_ID
),
budget_data AS (
    SELECT 
        b.PRODUCT_ID,
        SUM(b.QUANTITY) AS BUDGET,
        b.MONTH,
        b.YEAR
    FROM {{ ref('stg_google_sheets_budget') }} b
    GROUP BY b.PRODUCT_ID, b.MONTH, b.YEAR
)
SELECT 
    p.PRODUCT_ID,
    p.NAME,
    p.PRICE,
    p.INVENTORY,
    COALESCE(sd.QUANTITY_SOLD, 0) AS QUANTITY_SOLD,
    COALESCE(sd.REVENUE, 0) AS REVENUE,
    COALESCE(pu.PROMO_USAGE, 0) AS PROMO_USAGE,
    COALESCE(b.BUDGET, 0) AS BUDGET,
    b.MONTH,
    b.YEAR
FROM {{ ref('stg_sql_server_dbo__products') }} p
LEFT JOIN sales_data sd
    ON p.PRODUCT_ID = sd.PRODUCT_ID
LEFT JOIN promo_usage pu
    ON p.PRODUCT_ID = pu.PRODUCT_ID
LEFT JOIN budget_data b
    ON p.PRODUCT_ID = b.PRODUCT_ID
