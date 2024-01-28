{{ config(alias='payrun_total_history_source', materialized = 'table') }}
select {{ dbt_utils.star(from=source('keypay_s3', 'payrun_total_history')) }}
from {{ source('keypay_s3', 'payrun_total_history') }}
