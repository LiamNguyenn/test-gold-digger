{{ config(alias='super_details_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'super_details'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'super_details')) }}
from {{ source('keypay_s3', 'super_details') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
