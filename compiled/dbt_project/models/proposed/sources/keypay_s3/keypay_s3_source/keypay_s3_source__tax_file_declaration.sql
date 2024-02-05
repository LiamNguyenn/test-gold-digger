

select
    "id",
  "employee_id",
  "employment_type_id",
  "from_date",
  "to_date",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."tax_file_declaration" where date_trunc('day', _transaction_date) = '2024-02-01'