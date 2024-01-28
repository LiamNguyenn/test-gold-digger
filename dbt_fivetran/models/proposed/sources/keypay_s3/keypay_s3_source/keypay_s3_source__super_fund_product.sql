{{ config(alias='super_fund_product_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'super_fund_product'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'super_fund_product')) }}
from {{ source('keypay_s3', 'super_fund_product') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
