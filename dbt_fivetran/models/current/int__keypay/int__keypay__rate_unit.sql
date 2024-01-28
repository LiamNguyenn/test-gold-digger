{{ config(alias='rate_unit', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__rate_unit')) %}
select * from {{ ref('stg__keypay__rate_unit') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
