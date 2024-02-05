

select
    "id",
  "status",
  "is_test",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."pay_run_lodgement_data" where date_trunc('day', _transaction_date) = '2024-01-14'