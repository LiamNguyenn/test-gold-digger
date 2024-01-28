{{ config(alias='pay_run_lodgement_data', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__pay_run_lodgement_data')) %}
select * from {{ ref('stg__keypay__pay_run_lodgement_data') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
