{{ config(materialized='table') }}

WITH sales AS (
    SELECT 
        d.DATE_DAY,
        d.YEAR_NUMBER,
        d.PRIOR_YEAR_DATE_DAY,
        SUM(o.ORDER_TOTAL) AS TOTAL_SALES
    FROM {{ ref('dim_date') }} d
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders') }} o
        ON d.DATE_DAY = CAST(o.CREATED_AT AS DATE)
    WHERE o._FIVETRAN_DELETED = FALSE
    GROUP BY d.DATE_DAY, d.YEAR_NUMBER, d.PRIOR_YEAR_DATE_DAY
),
prior_year_sales AS (
    SELECT 
        d.PRIOR_YEAR_DATE_DAY AS DATE_DAY,
        SUM(o.ORDER_TOTAL) AS PRIOR_YEAR_TOTAL_SALES
    FROM {{ ref('dim_date') }} d
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders') }} o
        ON d.PRIOR_YEAR_DATE_DAY = CAST(o.CREATED_AT AS DATE)
    WHERE o._FIVETRAN_DELETED = FALSE
    GROUP BY d.PRIOR_YEAR_DATE_DAY
)
SELECT 
    s.DATE_DAY,
    s.YEAR_NUMBER,
    s.TOTAL_SALES,
    p.PRIOR_YEAR_TOTAL_SALES,
    (s.TOTAL_SALES - COALESCE(p.PRIOR_YEAR_TOTAL_SALES, 0)) AS YEARLY_DIFFERENCE
FROM sales s
LEFT JOIN prior_year_sales p
    ON s.DATE_DAY = p.DATE_DAY
