select
    *,
    {{ get_date_id('report_date') }} as dim_date_sk
from {{ ref("int_enrich_date_spine") }}
