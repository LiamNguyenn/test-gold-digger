{{ config(alias='expense', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__expense')) %}
select * from {{ ref('stg__keypay__expense') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
