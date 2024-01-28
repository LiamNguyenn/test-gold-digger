{{ config(alias='pay_day_filing', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__pay_day_filing')) %}
select * from {{ ref('stg__keypay__pay_day_filing') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
