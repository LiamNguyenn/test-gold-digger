{{ config(alias='all_checks_results', materialized = 'table') }}
select * from {{ source('stg_checkly', 'all_checks_results') }}