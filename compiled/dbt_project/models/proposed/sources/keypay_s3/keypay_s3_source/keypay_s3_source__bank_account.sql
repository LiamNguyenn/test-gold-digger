

select
    "id",
  "employee_id",
  "external_reference_id",
  "source",
  "account_type",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."bank_account" where date_trunc('day', _transaction_date) = '2024-02-01'