{{ config(materialized='table') }}

 SELECT 
        ADDRESS_ID,
        CAST( ZIPCODE AS INT) AS ZIPCODE,
        COUNTRY,
        ADDRESS,
        replace(SPLIT(address, ' ')[0],'"','') AS numero_calle,
        STATE,
        _FIVETRAN_SYNCED
    FROM {{ source('sql_server_dbo', 'addresses') }} 

    --fivetran_deleted  borrar porque todo null