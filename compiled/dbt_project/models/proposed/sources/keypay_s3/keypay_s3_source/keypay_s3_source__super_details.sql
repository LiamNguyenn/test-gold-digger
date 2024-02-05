

select
    "id",
  "business_id",
  "date_registered_utc",
  "enabled",
  "date_beam_terms_accepted_utc",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."super_details" where date_trunc('day', _transaction_date) = '2024-02-01'