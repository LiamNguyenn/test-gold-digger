{{ config(alias='pay_category', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__pay_category')) %}
select * from {{ ref('stg__keypay__pay_category') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'