{{ config(alias='fair_work_award_selection_source', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date(source('keypay_s3', 'fair_work_award_selection'), '_transaction_date') %}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'fair_work_award_selection')) }}
from {{ source('keypay_s3', 'fair_work_award_selection') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
