{{ config(materialized='table') }}

WITH weekly_sales AS (
    SELECT 
        d.WEEK_START_DATE,
        d.WEEK_END_DATE,
        d.WEEK_OF_YEAR,
        d.YEAR_NUMBER,
        SUM(o.ORDER_TOTAL) AS WEEKLY_SALES,
        COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS
    FROM {{ ref('dim_date') }} d
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders') }} o
        ON d.DATE_DAY = CAST(o.CREATED_AT AS DATE)
    WHERE o._FIVETRAN_DELETED = FALSE
    GROUP BY d.WEEK_START_DATE, d.WEEK_END_DATE, d.WEEK_OF_YEAR, d.YEAR_NUMBER
)
SELECT 
    WEEK_START_DATE,
    WEEK_END_DATE,
    WEEK_OF_YEAR,
    YEAR_NUMBER,
    WEEKLY_SALES,
    TOTAL_ORDERS
FROM weekly_sales
