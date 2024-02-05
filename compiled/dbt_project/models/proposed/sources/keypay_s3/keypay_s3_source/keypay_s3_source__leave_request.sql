

select
    "id",
  "employee_id",
  "from_date",
  "to_date",
  "total_hours",
  "requested_date",
  "status",
  "business_id",
  "leave_category_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."leave_request" where date_trunc('day', _transaction_date) = '2024-02-01'