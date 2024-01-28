{{ config(alias='earnings_line_ext_au_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'earnings_line_ext_au'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'earnings_line_ext_au')) }}
from {{ source('keypay_s3', 'earnings_line_ext_au') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
