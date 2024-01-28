{{ config(alias='employment_agreement', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__employment_agreement')) %}
select * from {{ ref('stg__keypay__employment_agreement') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
