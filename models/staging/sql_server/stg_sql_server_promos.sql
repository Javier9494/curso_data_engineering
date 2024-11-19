{{ config(materialized='table') }}

WITH t1 as(
     SELECT 
        *
    FROM {{ source('sql_server_dbo', 'promos') }} 
UNION ALL
    SELECT
    'no promo' AS promo_id, 
    '0' AS discount,         
    'inactivo' AS status,     
    NULL AS _fivetran_deleted, 
    CURRENT_TIMESTAMP AS _fivetran_synced 
)


,base AS (
    SELECT 
        *,
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_surr_key, 
        promo_id AS promo_desc 
    FROM t1
)

SELECT 
    promo_surr_key, 
    promo_desc,     
    discount,       
    status,
    _fivetran_deleted,
    _fivetran_synced
FROM base


