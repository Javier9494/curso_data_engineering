{{ config(materialized='table') }}

 SELECT 
       *
    FROM {{ source('sql_server_dbo', 'events') }} 

    --fivetran_deleted  borrar porque todo null