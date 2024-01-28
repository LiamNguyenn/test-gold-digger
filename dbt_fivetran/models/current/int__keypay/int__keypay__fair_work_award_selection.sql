{{ config(alias='fair_work_award_selection', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__fair_work_award_selection')) %}
select * from {{ ref('stg__keypay__fair_work_award_selection') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
