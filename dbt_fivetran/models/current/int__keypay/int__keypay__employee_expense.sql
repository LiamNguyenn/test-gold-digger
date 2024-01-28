{{ config(alias='employee_expense', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__employee_expense')) %}
select * from {{ ref('stg__keypay__employee_expense') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
