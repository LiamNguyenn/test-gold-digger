

select
    "id",
  "businessid",
  "extracted_at",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."aba_details" where date_trunc('day', _transaction_date) = '2024-02-02'