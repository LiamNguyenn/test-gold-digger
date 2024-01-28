{{ config(alias='device_token_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'device_token'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'device_token')) }}
from {{ source('keypay_s3', 'device_token') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
