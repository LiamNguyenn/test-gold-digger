{{ config(  enabled=false) }}

select table_id from {{ source('csv', 'test_connector_postgres') }} 