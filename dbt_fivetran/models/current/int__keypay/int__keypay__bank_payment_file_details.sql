{{ config(alias='bank_payment_file_details', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__bank_payment_file_details')) %}
select * from {{ ref('stg__keypay__bank_payment_file_details') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
