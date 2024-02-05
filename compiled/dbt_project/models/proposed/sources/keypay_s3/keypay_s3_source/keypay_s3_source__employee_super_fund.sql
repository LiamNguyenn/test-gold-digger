

select
    "id",
  "super_fund_name",
  "member_number",
  "allocated_percentage",
  "fixed_amount",
  "employee_id",
  "deleted",
  "super_fund_product_id",
  "has_non_super_stream_compliant_fund",
  "date_employee_nominated_utc",
  "super_details_default_fund_id",
  "self_managed_super_fund_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "allocate_balance"
from "dev"."keypay_s3"."employee_super_fund" where date_trunc('day', _transaction_date) = '2024-02-01'