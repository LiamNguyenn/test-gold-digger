



select
    1
from (select * from "dev"."exports"."exports_braze_users" where alpha_two_letter is not null) dbt_subquery

where not(country is not null)

