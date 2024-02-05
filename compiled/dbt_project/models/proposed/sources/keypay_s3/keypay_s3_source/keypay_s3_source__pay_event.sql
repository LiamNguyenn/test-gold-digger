

select
    "id",
  "business_id",
  "date_created_utc",
  "status",
  "pay_run_id",
  "date_lodged_utc",
  "date_response_received_utc",
  "pay_run_lodgement_data_id",
  "is_deleted",
  "stp_version",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."pay_event" where date_trunc('day', _transaction_date) = '2024-02-01'