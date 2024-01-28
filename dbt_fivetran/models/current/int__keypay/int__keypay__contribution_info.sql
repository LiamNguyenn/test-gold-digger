{{ config(alias='contribution_info', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__contribution_info')) %}
select * from {{ ref('stg__keypay__contribution_info') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
