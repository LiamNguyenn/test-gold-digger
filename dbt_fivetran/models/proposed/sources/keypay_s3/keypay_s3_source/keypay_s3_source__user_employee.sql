{{ config(alias='user_employee_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'user_employee'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'user_employee')) }}
from {{ source('keypay_s3', 'user_employee') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
