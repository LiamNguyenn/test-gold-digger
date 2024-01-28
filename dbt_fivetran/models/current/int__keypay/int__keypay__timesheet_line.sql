{{ config(alias='timesheet_line', materialized = 'table') }}
{% set latest_transaction_date = get_latest_transaction_date_v2(ref('stg__keypay__timesheet_line')) %}
select * from {{ ref('stg__keypay__timesheet_line') }} where date_trunc('day', _transaction_date) = '{{ latest_transaction_date }}'
