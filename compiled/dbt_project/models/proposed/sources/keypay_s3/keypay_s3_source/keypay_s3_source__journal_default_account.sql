

select
    "id",
  "business_id",
  "account_type",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."journal_default_account" where date_trunc('day', _transaction_date) = '2024-02-02'