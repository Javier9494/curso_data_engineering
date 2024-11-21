{{ config(materialized='table') }}


    SELECT 
       _row,
       CAST(quantity AS INT) AS quantity,
       month(month) as month,
       year(month) as year,
       product_id,
       _fivetran_synced
    FROM {{ source('google_sheets', 'budget') }} -- Fuente original
