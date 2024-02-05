

select
    "id",
  "user_id",
  "platform",
  "endpoint",
  "date_created_utc",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."device_token" where date_trunc('day', _transaction_date) = '2024-01-29'