{{ config(alias='leave_allowance_template_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'leave_allowance_template'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'leave_allowance_template')) }}
from {{ source('keypay_s3', 'leave_allowance_template') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
