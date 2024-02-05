

select
    "id",
  "name",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."fair_work_award" where date_trunc('day', _transaction_date) = '2024-02-01'