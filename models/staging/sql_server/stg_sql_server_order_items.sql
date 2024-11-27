{{ config(materialized='table') }}

 SELECT 
       *
    FROM {{ source('sql_server_dbo', 'order_items') }} 

    --fivetran_deleted  borrar porque todo null