

select
    "id",
  "user_id",
  "employee_group_id",
  "permissions",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."user_employee_group" where date_trunc('day', _transaction_date) = '2024-02-01'