

select
    "id",
  "employee_id",
  "start_time",
  "end_time",
  "units",
  "date_created",
  "submitted_start_time",
  "submitted_end_time",
  "pay_category_id",
  "status",
  "leave_request_id",
  "consolidated_with_timesheet_line_id",
  "pay_run_total_id",
  "business_id",
  "auto_approved_by_roster_shift_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."timesheet_line" where date_trunc('day', _transaction_date) = '2023-11-08'