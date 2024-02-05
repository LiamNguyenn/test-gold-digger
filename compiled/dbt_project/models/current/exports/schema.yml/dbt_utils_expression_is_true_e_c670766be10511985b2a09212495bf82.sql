



select
    1
from (select * from "dev"."exports"."exports_braze_users" where marketing_consented_at is null) dbt_subquery

where not(is_marketing_consented  = false)

