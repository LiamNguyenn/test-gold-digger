

select
    "id",
  "name",
  "date_created_utc",
  "fair_work_award_id",
  "is_disabled",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."award_package" where date_trunc('day', _transaction_date) = '2024-02-01'