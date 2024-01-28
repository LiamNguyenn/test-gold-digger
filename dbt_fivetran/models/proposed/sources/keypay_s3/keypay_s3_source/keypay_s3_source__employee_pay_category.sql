{{ config(alias='employee_pay_category_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'employee_pay_category'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'employee_pay_category')) }}
from {{ source('keypay_s3', 'employee_pay_category') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
