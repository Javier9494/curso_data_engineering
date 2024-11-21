{{ config(materialized='table') }}

 SELECT 
        user_id,
        UPDATED_AT,
        ADDRESS_ID,
        LAST_NAME,
        CREATED_AT,
        PHONE_NUMBER,
        FIRST_NAME,
        EMAIL,
       -- _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED
    FROM {{ source('sql_server_dbo', 'users') }} 

    -- borramos campo total_orders, ya que todo nulo y ver que hacer con fivetran_deleted ya que también todo nulo
    -- tiene sentido?