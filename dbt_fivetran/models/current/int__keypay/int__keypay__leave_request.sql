{{ config(alias='leave_request', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__leave_request')) %}
select * from {{ ref('stg__keypay__leave_request') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
