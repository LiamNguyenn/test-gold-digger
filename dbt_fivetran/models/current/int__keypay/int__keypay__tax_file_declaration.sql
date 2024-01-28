{{ config(alias='tax_file_declaration', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__tax_file_declaration')) %}
select * from {{ ref('stg__keypay__tax_file_declaration') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
