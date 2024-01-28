{{ config(alias='super_details_default_fund', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__super_details_default_fund')) %}
select * from {{ ref('stg__keypay__super_details_default_fund') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
