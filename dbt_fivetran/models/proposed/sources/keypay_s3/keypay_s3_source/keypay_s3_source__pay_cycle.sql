{{ config(alias='pay_cycle_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'pay_cycle'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'pay_cycle')) }}
from {{ source('keypay_s3', 'pay_cycle') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
