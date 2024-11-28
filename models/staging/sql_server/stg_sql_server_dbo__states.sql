{{ config(materialized='table') }}


with state_cte AS (
    SELECT 
        -- normalizamos state porque vamos a hacer consultas por este campo con regularidad.
        {{ dbt_utils.generate_surrogate_key(['STATE']) }} AS state_id,
        STATE
    FROM {{ ref('base_sql_server_dbo_addresses') }} 
)
 SELECT *
    FROM state_cte