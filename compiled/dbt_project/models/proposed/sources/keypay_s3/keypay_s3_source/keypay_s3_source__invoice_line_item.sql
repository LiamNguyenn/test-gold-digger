

select
    "id",
  "invoice_id",
  "total_including_gst",
  "abn",
  "business_id",
  "quantity",
  "billing_code",
  "billing_plan",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "unit_price_including_gst"
from "dev"."keypay_s3"."invoice_line_item" where date_trunc('day', _transaction_date) = '2023-11-14'