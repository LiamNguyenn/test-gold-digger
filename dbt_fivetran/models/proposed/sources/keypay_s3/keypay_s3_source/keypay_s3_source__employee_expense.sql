{{ config(alias='employee_expense_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'employee_expense'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'employee_expense')) }}
from {{ source('keypay_s3', 'employee_expense') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
