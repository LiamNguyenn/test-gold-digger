{{ config(alias='statutory_settings', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__statutory_settings')) %}
select * from {{ ref('stg__keypay__statutory_settings') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
