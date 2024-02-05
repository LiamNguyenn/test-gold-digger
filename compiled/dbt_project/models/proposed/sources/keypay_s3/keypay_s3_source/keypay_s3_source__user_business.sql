

select
    "user_id",
  "business_id",
  "is_single_sign_on_enabled",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."user_business" where date_trunc('day', _transaction_date) = '2024-02-01'