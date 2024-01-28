{{ config(alias='bank_account_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'bank_account'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'bank_account')) }}
from {{ source('keypay_s3', 'bank_account') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
