{{ config(alias='pay_cycle', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__pay_cycle')) %}
select * from {{ ref('stg__keypay__pay_cycle') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
