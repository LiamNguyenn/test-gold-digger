{{ config(alias='zoom_info_source', materialized = 'table') }}
select {{ dbt_utils.star(from=source('keypay_s3', 'zoom_info')) }}
from {{ source('keypay_s3', 'zoom_info') }}
