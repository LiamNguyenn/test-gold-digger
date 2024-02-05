

select
    "id",
  "business_id",
  "pay_cycle_frequencyid",
  "name",
  "last_pay_run",
  "is_deleted",
  "aba_detailsid",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."pay_cycle" where date_trunc('day', _transaction_date) = '2024-02-02'