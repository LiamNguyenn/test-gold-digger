{{ config(alias='suburb', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay_dwh__suburb')) %}
select *
from {{ ref('stg__keypay_dwh__suburb') }}
where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'