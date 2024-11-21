{{ config(materialized='table') }}

 SELECT 
        PRODUCT_ID,
        CAST(PRICE AS FLOAT) AS PRICE,
        NAME,
        CAST(INVENTORY AS INT) AS INVENTORY,
        _FIVETRAN_SYNCED
    FROM {{ source('sql_server_dbo', 'products') }} 

    -- quitar total_orders ya que todo null, lo mismo con _FIVETRAN_DELETED