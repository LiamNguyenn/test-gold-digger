{{ config(alias='leave_request_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'leave_request'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'leave_request')) }}
from {{ source('keypay_s3', 'leave_request') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
