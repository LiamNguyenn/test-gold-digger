{{ config(alias='user_report_access_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'user_report_access'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'user_report_access')) }}
from {{ source('keypay_s3', 'user_report_access') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
