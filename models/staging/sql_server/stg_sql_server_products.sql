{{ config(materialized='table') }}

 SELECT 
        *
    FROM {{ source('sql_server_dbo', 'products') }} 