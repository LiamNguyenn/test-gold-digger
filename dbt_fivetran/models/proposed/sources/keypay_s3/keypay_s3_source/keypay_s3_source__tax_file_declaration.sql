{{ config(alias='tax_file_declaration_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'tax_file_declaration'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'tax_file_declaration')) }}
from {{ source('keypay_s3', 'tax_file_declaration') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
