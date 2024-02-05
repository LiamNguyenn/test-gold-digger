

select
    "id",
  "pay_run_total_id",
  "employee_super_fund_id",
  "amount",
  "pay_run_id",
  "employee_id",
  "contribution_type",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."super_payment" where date_trunc('day', _transaction_date) = '2024-02-01'