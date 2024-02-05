

select
    "id",
  "business_id",
  "file_format",
  "originating_account_number",
  "originating_account_name",
  "lodgement_reference",
  "merge_multiple_account_payments",
  "payment_additional_content",
  "transaction_reference_number",
  "is_confidential",
  "is_payment_integration",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."bank_payment_file_details" where date_trunc('day', _transaction_date) = '2024-02-02'