

select
    "id",
  "fair_work_award_id",
  "business_id",
  "date_time_utc",
  "source",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."fair_work_award_selection" where date_trunc('day', _transaction_date) = '2024-02-01'