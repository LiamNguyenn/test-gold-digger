

select
    "id",
  "name",
  "businessid",
  "is_deleted",
  "parentid",
  "date_created",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."location" where date_trunc('day', _transaction_date) = '2024-02-02'