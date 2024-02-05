

select
    "id",
  "employee_id",
  "employee_history_action_id",
  "date_created",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."employee_history" where date_trunc('day', _transaction_date) = '2024-02-01'