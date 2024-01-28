{{ config(alias='contribution_info_deduction_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'contribution_info_deduction'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'contribution_info_deduction')) }}
from {{ source('keypay_s3', 'contribution_info_deduction') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
