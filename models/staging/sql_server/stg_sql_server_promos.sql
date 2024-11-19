{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        *,
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_surr_key, -- Genera la clave surrogada
        promo_id AS promo_desc -- Copia el valor de promo_id a promo_desc
    FROM {{ source('sql_server_dbo', 'promos') }} -- Fuente original
)

SELECT 
    promo_surr_key, -- Nueva clave surrogada
    promo_id,       -- Columna original
    promo_desc,     -- Nueva columna
    discount,       -- Mantén el resto de las columnas
    status,
    _fivetran_deleted,
    _fivetran_synced
FROM base;
