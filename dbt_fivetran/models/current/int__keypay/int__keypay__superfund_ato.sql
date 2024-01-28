{{ config(alias='superfund_ato', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__superfund_ato')) %}
select * from {{ ref('stg__keypay__superfund_ato') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
