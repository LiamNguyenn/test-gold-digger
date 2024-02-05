

select
    "contribution_info_id",
  "deduction_id",
  "failed",
  "id",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."contribution_info_deduction" where date_trunc('day', _transaction_date) = '2024-02-02'