{{ config(alias='suburb_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'suburb'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'suburb')) }}
from {{ source('keypay_s3', 'suburb') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
