{{ config(alias='super_details_default_fund_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'super_details_default_fund'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'super_details_default_fund')) }}
from {{ source('keypay_s3', 'super_details_default_fund') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'