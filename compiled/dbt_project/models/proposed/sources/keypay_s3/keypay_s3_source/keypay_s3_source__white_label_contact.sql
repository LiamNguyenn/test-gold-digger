

select
    "id",
  "white_label_id",
  "user_id",
  "contact_type",
  "name",
  "email",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified"
from "dev"."keypay_s3"."white_label_contact" where date_trunc('day', _transaction_date) = '2024-02-01'