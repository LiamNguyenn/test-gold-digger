{{ config(alias='employee_history', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__employee_history')) %}
select * from {{ ref('stg__keypay__employee_history') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
