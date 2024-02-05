

select
    "id",
  "business_id",
  "classification",
  "date_created_utc",
  "external_reference_id",
  "is_deleted",
  "business_award_package_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."employment_agreement" where date_trunc('day', _transaction_date) = '2024-02-01'