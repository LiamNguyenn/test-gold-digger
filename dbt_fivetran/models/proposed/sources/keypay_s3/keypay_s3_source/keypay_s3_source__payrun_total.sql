{{ config(alias='payrun_total_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'payrun_total'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'payrun_total')) }}
from {{ source('keypay_s3', 'payrun_total') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
