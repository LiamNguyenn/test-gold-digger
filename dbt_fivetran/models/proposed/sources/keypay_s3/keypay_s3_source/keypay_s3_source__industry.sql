{{ config(alias='industry_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'industry'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'industry')) }}
from {{ source('keypay_s3', 'industry') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
