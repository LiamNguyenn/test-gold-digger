
select
    "id",
  "name",
  "billing_name",
  "date_created_utc",
  "commence_billing_from",
  "_file",
  "_transaction_date",
  "_etl_date",
  "_modified",
  "default_region_id"
from "dev"."keypay_s3"."resellers"
where 1 = 1


        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from "dev"."keypay_s3"."resellers_source")


qualify row_number() over (partition by id order by _transaction_date desc) = 1