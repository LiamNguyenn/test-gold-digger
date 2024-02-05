

select
    "id",
  "business_id",
  "user_id",
  "filter_type",
  "value",
  "permissions",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."location_restriction" where date_trunc('day', _transaction_date) = '2024-02-01'