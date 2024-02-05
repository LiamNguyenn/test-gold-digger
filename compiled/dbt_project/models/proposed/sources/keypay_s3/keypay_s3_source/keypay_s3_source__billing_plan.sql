

select
    "id",
  "name",
  "function_employee_onboarding",
  "price_per_unit",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."billing_plan" where date_trunc('day', _transaction_date) = '2024-02-01'