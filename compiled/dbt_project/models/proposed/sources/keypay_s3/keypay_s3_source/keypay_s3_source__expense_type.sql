

select
    "id",
  "description",
  "unit_cost",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."expense_type" where date_trunc('day', _transaction_date) = '2024-02-01'