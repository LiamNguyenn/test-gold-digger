



select
    1
from (select * from "dev"."exports"."exports_braze_users" where user_actively_employed is true) dbt_subquery

where not(last_name is not null)

