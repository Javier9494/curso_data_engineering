
{{
    config(
        materialized = "table"
    )
}}

{{ dbt_date.get_date_dimension("2020-01-01", "2024-12-31") }}


--uso paquete dbt_date para generar la dim_tiempo  automáticamente