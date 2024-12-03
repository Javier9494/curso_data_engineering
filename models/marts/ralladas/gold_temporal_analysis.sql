{{ config(materialized='table') }}

WITH sales AS (
    SELECT 
        d.DATE_DAY,
        d.DAY_OF_WEEK_NAME,
        d.MONTH_NAME,
        d.YEAR_NUMBER,
        SUM(o.ORDER_TOTAL) AS TOTAL_SALES,
        SUM(o.ORDER_COST) AS TOTAL_COST,
        COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS
    FROM {{ ref('dim_date') }} d
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders') }} o
        ON d.DATE_DAY = CAST(o.CREATED_AT AS DATE)
    WHERE o._FIVETRAN_DELETED = FALSE
    GROUP BY d.DATE_DAY, d.DAY_OF_WEEK_NAME, d.MONTH_NAME, d.YEAR_NUMBER
)
SELECT 
    DATE_DAY,
    DAY_OF_WEEK_NAME,
    MONTH_NAME,
    YEAR_NUMBER,
    TOTAL_SALES,
    TOTAL_COST,
    TOTAL_ORDERS
FROM sales
