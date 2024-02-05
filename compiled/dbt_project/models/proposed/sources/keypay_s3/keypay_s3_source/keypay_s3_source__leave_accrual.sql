
select
    "id",
  "employee_id",
  "accrued_amount",
  "accrual_status_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."leave_accrual"
where 1 = 1


        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from "dev"."keypay_s3"."leave_accrual_source")

    
qualify row_number() over (partition by id, employee_id order by _transaction_date desc) = 1