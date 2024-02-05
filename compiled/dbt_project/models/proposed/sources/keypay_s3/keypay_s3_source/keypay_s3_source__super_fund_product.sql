

select
    "id",
  "abn",
  "product_code",
  "product_type",
  "business_name",
  "product_name",
  "source",
  "business_id",
  "super_stream_status",
  "is_managed_by_system",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."super_fund_product" where date_trunc('day', _transaction_date) = '2024-02-01'