
select "id",
  "employee_id",
  "pay_category_id",
  "pay_run_id",
  "units",
  "location_id",
  "pay_run_total_id",
  "rate",
  "earnings_line_status_id",
  "external_reference_id",
  "net_earnings",
  "net_earnings_reporting",
  "earnings_line_ext_au_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."earnings_line"


  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where _transaction_date > (select max(_transaction_date) from "dev"."keypay_s3"."earnings_line_source")

