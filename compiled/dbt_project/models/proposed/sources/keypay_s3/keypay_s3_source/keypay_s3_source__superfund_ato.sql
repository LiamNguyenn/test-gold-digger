

select
    "abn",
  "fund_name",
  "usi",
  "product_name",
  "contribution_restrictions",
  "from_date",
  "to_date",
  "_transaction_date",
  "_etl_date"
from "dev"."keypay_s3"."superfund_ato" where date_trunc('day', _transaction_date) = '2024-02-04'