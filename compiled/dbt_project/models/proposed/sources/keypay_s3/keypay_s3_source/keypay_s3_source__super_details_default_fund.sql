

select
    "id",
  "super_details_id",
  "usi",
  "abn",
  "name",
  "is_deleted",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."super_details_default_fund" where date_trunc('day', _transaction_date) = '2024-02-01'