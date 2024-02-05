

select
    "id",
  "expense_date",
  "business_id",
  "unit_cost",
  "quantity",
  "invoice_id",
  "notes",
  "expense_type",
  "displayed_unit_cost",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."expense" where date_trunc('day', _transaction_date) = '2024-02-01'