{{ config(alias='accrual_status_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'accrual_status'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'accrual_status')) }}
from {{ source('keypay_s3', 'accrual_status') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
