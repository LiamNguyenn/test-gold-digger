{{ config(alias='pay_cycle_frequency_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'pay_cycle_frequency'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'pay_cycle_frequency')) }}
from {{ source('keypay_s3', 'pay_cycle_frequency') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
