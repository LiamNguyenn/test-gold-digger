

select
    "id",
  "business_id",
  "cpf_submission_number",
  "csn_type",
  "is_deleted",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."csn_details" where date_trunc('day', _transaction_date) = '2024-02-02'