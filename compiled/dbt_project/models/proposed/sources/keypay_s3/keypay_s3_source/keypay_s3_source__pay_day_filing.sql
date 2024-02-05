

select
    "id",
  "business_id",
  "pay_run_id",
  "status",
  "date_last_modified",
  "date_submitted",
  "version",
  "pay_day_filing_lodgement_data_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."pay_day_filing" where date_trunc('day', _transaction_date) = '2024-02-01'