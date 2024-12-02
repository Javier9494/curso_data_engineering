{% snapshot sql_server_users_snp %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='_FIVETRAN_SYNCED'
    )
}}

SELECT 
    *
        
      FROM {{ source('sql_server_dbo', 'users') }} 


{% endsnapshot %}