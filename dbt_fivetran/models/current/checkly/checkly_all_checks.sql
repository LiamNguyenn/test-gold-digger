{{ config(alias='all_checks', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(source('stg_checkly', 'all_checks')) %}
select * from {{ source('stg_checkly', 'all_checks') }}
         where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'