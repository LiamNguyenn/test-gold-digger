

select
    "id",
  "leave_category_name",
  "business_id",
  "exclude_from_termination_payout",
  "is_deleted",
  "unit_type",
  "source",
  "date_created",
  "deduct_from_primary_pay_category",
  "deduct_from_pay_category_id",
  "transfer_to_pay_category_id",
  "leave_category_type",
  "entitlement_period",
  "contingent_period",
  "automatically_accrues",
  "standard_hours_per_year",
  "units",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "is_balance_untracked"
from "dev"."keypay_s3"."leave_category" where date_trunc('day', _transaction_date) = '2024-02-01'