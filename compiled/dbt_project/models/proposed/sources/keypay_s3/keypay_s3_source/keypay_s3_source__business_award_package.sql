

select
    "id",
  "business_id",
  "award_package_id",
  "current_version_id",
  "award_package_name",
  "installation_status",
  "installation_status_last_updated_utc",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."business_award_package" where date_trunc('day', _transaction_date) = '2024-02-01'