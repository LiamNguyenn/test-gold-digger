{{ config(alias='invoice_line_item', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__invoice_line_item')) %}
select * from {{ ref('stg__keypay__invoice_line_item') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
