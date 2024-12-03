{{ config(materialized='table') }}

WITH user_data AS (
    SELECT 
        u.USER_ID,
        u.FIRST_NAME,
        u.LAST_NAME,
        u.EMAIL,
        u.PHONE_NUMBER,
        u.ADDRESS_ID,
        a.COUNTRY,
        a.STATE,
        a.ZIPCODE
    FROM {{ ref('stg_sql_server_dbo__users') }} u
    LEFT JOIN {{ ref('stg_sql_server_dbo__addresses') }} a
        ON u.ADDRESS_ID = a.ADDRESS_ID
    WHERE u._FIVETRAN_DELETED = FALSE
),
events AS (
    SELECT 
        e.USER_ID,
        COUNT(*) AS TOTAL_EVENTS,
        MAX(e.CREATED_AT) AS LAST_EVENT_AT
    FROM {{ ref('stg_sql_server_dbo__events') }} e
    WHERE e._FIVETRAN_DELETED = FALSE
    GROUP BY e.USER_ID
),
spending AS (
    SELECT 
        o.USER_ID,
        SUM(o.ORDER_TOTAL) AS TOTAL_SPENT
    FROM {{ ref('stg_sql_server_dbo__orders') }} o
    WHERE o._FIVETRAN_DELETED = FALSE
    GROUP BY o.USER_ID
)
SELECT 
    ud.USER_ID,
    ud.FIRST_NAME,
    ud.LAST_NAME,
    ud.EMAIL,
    ud.PHONE_NUMBER,
    ud.ADDRESS_ID,
    ud.COUNTRY,
    ud.STATE,
    ud.ZIPCODE,
    COALESCE(e.TOTAL_EVENTS, 0) AS TOTAL_EVENTS,
    e.LAST_EVENT_AT,
    COALESCE(s.TOTAL_SPENT, 0) AS TOTAL_SPENT
FROM user_data ud
LEFT JOIN events e
    ON ud.USER_ID = e.USER_ID
LEFT JOIN spending s
    ON ud.USER_ID = s.USER_ID
