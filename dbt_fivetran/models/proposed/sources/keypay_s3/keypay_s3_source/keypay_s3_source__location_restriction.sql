{{ config(alias='location_restriction_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'location_restriction'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'location_restriction')) }}
from {{ source('keypay_s3', 'location_restriction') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
