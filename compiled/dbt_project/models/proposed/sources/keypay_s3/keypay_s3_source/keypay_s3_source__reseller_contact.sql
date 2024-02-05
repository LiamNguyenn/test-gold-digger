

select
    "id",
  "reseller_id",
  "user_id",
  "contact_type",
  "name",
  "email",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."reseller_contact" where date_trunc('day', _transaction_date) = '2024-02-02'