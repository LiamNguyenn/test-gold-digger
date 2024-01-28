{{ config(alias='pay_day_filing_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'pay_day_filing'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'pay_day_filing')) }}
from {{ source('keypay_s3', 'pay_day_filing') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
