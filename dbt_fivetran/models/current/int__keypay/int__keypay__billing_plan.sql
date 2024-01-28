{{ config(alias='billing_plan', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__billing_plan')) %}
select * from {{ ref('stg__keypay__billing_plan') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
