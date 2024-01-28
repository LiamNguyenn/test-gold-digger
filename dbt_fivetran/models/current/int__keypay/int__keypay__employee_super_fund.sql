{{ config(alias='employee_super_fund', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__employee_super_fund')) %}
select * from {{ ref('stg__keypay__employee_super_fund') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
