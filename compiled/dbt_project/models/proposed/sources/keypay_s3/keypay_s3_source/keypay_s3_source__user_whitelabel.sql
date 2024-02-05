

select
    "user_id",
  "whitelabel_id",
  "is_default_parent",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "userid"
from "dev"."keypay_s3"."user_whitelabel" where date_trunc('day', _transaction_date) = '2024-02-01'