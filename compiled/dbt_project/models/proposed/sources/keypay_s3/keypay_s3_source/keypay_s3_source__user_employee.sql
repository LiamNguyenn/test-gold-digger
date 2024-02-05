

select
    "user_id",
  "employee_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."user_employee" where date_trunc('day', _transaction_date) = '2024-02-02'