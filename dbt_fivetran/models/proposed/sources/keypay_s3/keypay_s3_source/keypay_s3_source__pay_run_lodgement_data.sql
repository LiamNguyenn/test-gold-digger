{{ config(alias='pay_run_lodgement_data_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'pay_run_lodgement_data'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'pay_run_lodgement_data')) }}
from {{ source('keypay_s3', 'pay_run_lodgement_data') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
