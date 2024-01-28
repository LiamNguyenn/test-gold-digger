{{ config(alias='user_employee_group_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'user_employee_group'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'user_employee_group')) }}
from {{ source('keypay_s3', 'user_employee_group') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
