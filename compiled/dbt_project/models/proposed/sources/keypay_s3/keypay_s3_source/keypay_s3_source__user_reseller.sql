

select
    "userid",
  "reseller_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "user_id"
from "dev"."keypay_s3"."user_reseller" where date_trunc('day', _transaction_date) = '2024-02-01'