

select
    "id",
  "user_id",
  "business_id",
  "access_type",
  "no_reporting_restriction",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."user_report_access" where date_trunc('day', _transaction_date) = '2024-02-01'