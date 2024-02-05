

select
    "id",
  "business_id",
  "name",
  "external_reference_id",
  "source",
  "business_award_package_id",
  "leave_accrual_start_date_type",
  "leave_year_start",
  "leave_loading_calculated_from_pay_category_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."leave_allowance_template" where date_trunc('day', _transaction_date) = '2024-02-01'