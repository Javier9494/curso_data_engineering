{{ config(materialized='table') }}


    SELECT 
       _row,
       quantity,
       month(month) as month,
       year(month) as year,
       product_id,
       _fivetran_synced
    FROM {{ source('google_sheets', 'budget') }} -- Fuente original
