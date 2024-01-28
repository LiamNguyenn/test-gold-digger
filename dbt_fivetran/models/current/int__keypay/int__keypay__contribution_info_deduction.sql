{{ config(alias='contribution_info_deduction', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__contribution_info_deduction')) %}
select * from {{ ref('stg__keypay__contribution_info_deduction') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
