

select
    "id",
  "first_name",
  "last_name",
  "email",
  "is_active",
  "is_admin",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."user" where date_trunc('day', _transaction_date) = '2024-01-29'