

select
    "id",
  "business_id",
  "income_tax_number__encrypted",
  "e_number",
  "epf_number",
  "socso_number",
  "hrdf_status",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."statutory_settings" where date_trunc('day', _transaction_date) = '2024-02-02'