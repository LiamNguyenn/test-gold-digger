

select
    "id",
  "cont_amount",
  "cont_type",
  "super_member_id",
  "employee_id",
  "failed",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."contribution_info" where date_trunc('day', _transaction_date) = '2024-01-29'