{{ config(alias='superfund_ato_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'superfund_ato'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'superfund_ato')) }}
from {{ source('keypay_s3', 'superfund_ato') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
