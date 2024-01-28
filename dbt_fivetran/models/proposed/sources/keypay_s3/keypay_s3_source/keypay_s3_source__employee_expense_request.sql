{{ config(alias='employee_expense_request_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'employee_expense_request'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'employee_expense_request')) }}
from {{ source('keypay_s3', 'employee_expense_request') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
