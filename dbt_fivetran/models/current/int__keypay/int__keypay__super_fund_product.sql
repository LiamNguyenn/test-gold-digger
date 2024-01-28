{{ config(alias='super_fund_product', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__super_fund_product')) %}
select * from {{ ref('stg__keypay__super_fund_product') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
