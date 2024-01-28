{{ config(alias='location_restriction', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__location_restriction')) %}
select * from {{ ref('stg__keypay__location_restriction') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
