
select
    "id",
  "date_finalised",
  "pay_period_starting",
  "pay_period_ending",
  "date_paid",
  "business_id",
  "invoice_id",
  "date_first_finalised",
  "pay_run_lodgement_data_id",
  "notification_date",
  "finalised_by_id",
  "pay_cycle_id",
  "pay_cycle_frequency_id",
  "date_created_utc",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."payrun"
where 1 = 1


        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from "dev"."keypay_s3"."payrun_source")

    
qualify row_number() over (partition by id, business_id order by _transaction_date desc) = 1