{{ config(alias='user_reseller', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__user_reseller')) %}
select * from {{ ref('stg__keypay__user_reseller') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
