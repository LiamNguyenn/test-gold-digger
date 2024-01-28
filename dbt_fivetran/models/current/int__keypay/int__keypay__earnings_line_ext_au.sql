{{ config(alias='earnings_line_ext_au', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__earnings_line_ext_au')) %}
select * from {{ ref('stg__keypay__earnings_line_ext_au') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
