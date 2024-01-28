{{ config(
    alias="earnings_line",
    materialized="incremental",
) }}
select
    *
from {{ ref('stg__keypay__earnings_line') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where _transaction_date > (select max(_transaction_date) from {{ this }})

{% endif %}
