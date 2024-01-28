{{ config(alias='expense_type', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__expense_type')) %}
select * from {{ ref('stg__keypay__expense_type') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
