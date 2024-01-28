{{ config(alias='statutory_settings_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'statutory_settings'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'statutory_settings')) }}
from {{ source('keypay_s3', 'statutory_settings') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
