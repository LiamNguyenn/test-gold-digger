

select
    "id",
  "pay_run_id",
  "pay_run_total_id",
  "employee_id",
  "employee_expense_category_id",
  "location_id",
  "business_id",
  "amount",
  "notes",
  "external_id",
  "employee_recurring_expense_id",
  "employee_expense_request_id",
  "tax_code",
  "tax_rate",
  "tax_code_display_name",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."employee_expense" where date_trunc('day', _transaction_date) = '2024-02-02'