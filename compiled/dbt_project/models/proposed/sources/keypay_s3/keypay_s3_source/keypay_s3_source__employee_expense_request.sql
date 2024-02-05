

select
    "id",
  "employee_id",
  "status",
  "created_by_user_id",
  "date_created_utc",
  "status_updated_by_user_id",
  "date_status_updated_utc",
  "description",
  "status_update_notes",
  "pay_run_total_id",
  "business_id",
  "date_first_approved_utc",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."employee_expense_request" where date_trunc('day', _transaction_date) = '2024-02-02'