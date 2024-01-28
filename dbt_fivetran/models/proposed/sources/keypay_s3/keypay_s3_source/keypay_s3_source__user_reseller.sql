{{ config(alias='user_reseller_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'user_reseller'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'user_reseller')) }}
from {{ source('keypay_s3', 'user_reseller') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
