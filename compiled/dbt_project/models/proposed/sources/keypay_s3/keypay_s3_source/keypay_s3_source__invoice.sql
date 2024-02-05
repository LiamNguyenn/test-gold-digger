

select
    "id",
  "date",
  "gst_rate",
  "billing_region_id",
  "invoicee_id",
  "invoicee_type_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."invoice" where date_trunc('day', _transaction_date) = '2024-02-01'