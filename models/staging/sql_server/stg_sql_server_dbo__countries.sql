{{ config(materialized='table') }}


with country_cte AS (
    SELECT 
    -- normalizamos country porque vamos a hacer consultas por este campo con regularidad.
        {{ dbt_utils.generate_surrogate_key(['COUNTRY']) }} AS country_id,
        COUNTRY
    FROM {{ ref('base_sql_server_dbo_addresses') }} 
)
 SELECT *
    FROM country_cte