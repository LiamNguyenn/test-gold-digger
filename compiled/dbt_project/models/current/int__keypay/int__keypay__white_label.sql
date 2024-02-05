
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
from "dev"."stg__keypay"."white_label"
where
    1 = 1 

        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from "dev"."int__keypay"."white_label")


qualify row_number()
    over (partition by id order by _transaction_date desc)
= 1