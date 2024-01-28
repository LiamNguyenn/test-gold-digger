{{ config(alias='rate_unit_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'rate_unit'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'rate_unit')) }}
from {{ source('keypay_s3', 'rate_unit') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
