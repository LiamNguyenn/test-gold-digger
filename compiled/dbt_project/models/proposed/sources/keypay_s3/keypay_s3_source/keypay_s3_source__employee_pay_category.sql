

select
    "id",
  "calculated_rate",
  "employee_id",
  "standard_weekly_hours",
  "is_default",
  "from_date",
  "to_date",
  "user_supplied_rate",
  "standard_daily_hours",
  "pay_category_rate_unit_id",
  "employee_rate_unit_id",
  "expiry_date",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "pay_category_id"
from "dev"."keypay_s3"."employee_pay_category" where date_trunc('day', _transaction_date) = '2024-02-01'