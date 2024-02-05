

select
    "id",
  "employee_id",
  "from_date",
  "to_date",
  "job_title",
  "business_id",
  "default_pay_category_id",
  "is_payroll_tax_exempt",
  "employment_agreement_id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "default_pay_cycle_id"
from "dev"."keypay_s3"."payrun_default" where date_trunc('day', _transaction_date) = '2024-02-01'