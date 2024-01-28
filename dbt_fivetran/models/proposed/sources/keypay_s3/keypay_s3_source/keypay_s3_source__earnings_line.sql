{{ config(
    alias="earnings_line_source",
    materialized="incremental",
) }}
select {{ dbt_utils.star(from=source('keypay_s3', 'earnings_line')) }}
from {{ source('keypay_s3', 'earnings_line') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where _transaction_date > (select max(_transaction_date) from {{ this }})

{% endif %}
