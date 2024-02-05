
select id,
       currency,
       name,
       culture_name,
       default_standard_hours_per_day,
       commence_billing_from,
       minimum_bill_able_amount,
       "_file",
       "_transaction_date",
       "_etl_date",
       "_modified"
from (select id,
             currency,
             name,
             culture_name,
             default_standard_hours_per_day,
             commence_billing_from,
             minimum_bill_able_amount,
             "_file",
             "_transaction_date",
             "_etl_date",
             "_modified",
             row_number() over (partition by id order by _transaction_date desc) as seqnum
      from "dev"."stg__keypay"."region" t) t
where seqnum = 1 

        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from "dev"."int__keypay"."region")

