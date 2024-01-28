{{ config(alias='payrun_total', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__payrun_total')) %}
select * from {{ ref('stg__keypay__payrun_total') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
