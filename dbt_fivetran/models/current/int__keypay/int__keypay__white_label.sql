{{ config(alias="white_label", materialized="incremental", unique_key="id", incremental_strategy="delete+insert", on_schema_change= "sync_all_columns") }}
select
    id,
    name,
    is_deleted,
    region_id,
    support_email,
    primary_champion_id,
    function_enable_super_choice_marketplace,
    default_billing_plan_id,
    reseller_id,
    _file,
    _transaction_date,
    _etl_date,
    _modified
from {{ ref("stg__keypay__white_label") }}
where
    1 = 1 {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from {{ this }})

{% endif %}
qualify row_number()
    over (partition by id order by _transaction_date desc)
= 1
