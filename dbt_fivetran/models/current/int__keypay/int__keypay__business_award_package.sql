{{ config(alias='business_award_package', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__business_award_package')) %}
select * from {{ ref('stg__keypay__business_award_package') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
