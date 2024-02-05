

select
    "id",
  "employee_id",
  "payrun_id",
  "total_hours",
  "gross_earnings",
  "net_earnings",
  "is_excluded_from_billing",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."payrun_total" where date_trunc('day', _transaction_date) = '2024-02-01'