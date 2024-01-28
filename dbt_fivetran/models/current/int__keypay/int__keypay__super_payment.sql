{{ config(alias='super_payment', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__super_payment')) %}
select * from {{ ref('stg__keypay__super_payment') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
