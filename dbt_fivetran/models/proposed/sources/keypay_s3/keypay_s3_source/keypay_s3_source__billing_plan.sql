{{ config(alias='billing_plan_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'billing_plan'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'billing_plan')) }}
from {{ source('keypay_s3', 'billing_plan') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
