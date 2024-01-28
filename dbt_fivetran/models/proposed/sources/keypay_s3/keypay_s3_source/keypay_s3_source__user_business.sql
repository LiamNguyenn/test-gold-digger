{{ config(alias='user_business_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'user_business'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'user_business')) }}
from {{ source('keypay_s3', 'user_business') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
