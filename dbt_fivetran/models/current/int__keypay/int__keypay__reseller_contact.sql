{{ config(alias='reseller_contact', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__reseller_contact')) %}
select * from {{ ref('stg__keypay__reseller_contact') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
