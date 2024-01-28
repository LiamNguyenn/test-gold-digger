{{ config(alias='business_award_package_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'business_award_package'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'business_award_package')) }}
from {{ source('keypay_s3', 'business_award_package') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
