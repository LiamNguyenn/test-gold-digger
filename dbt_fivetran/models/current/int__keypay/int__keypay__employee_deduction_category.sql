{{ config(alias='employee_deduction_category', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__employee_deduction_category')) %}
select * from {{ ref('stg__keypay__employee_deduction_category') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
