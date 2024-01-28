{{ config(alias='white_label_contact', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__white_label_contact')) %}
select * from {{ ref('stg__keypay__white_label_contact') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
