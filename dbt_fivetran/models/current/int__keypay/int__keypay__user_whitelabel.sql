{{ config(alias='user_whitelabel', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__user_whitelabel')) %}
select * from {{ ref('stg__keypay__user_whitelabel') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
