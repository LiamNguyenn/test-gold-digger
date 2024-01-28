{{ config(alias='invoice_line_item_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'invoice_line_item'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'invoice_line_item')) }}
from {{ source('keypay_s3', 'invoice_line_item') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
