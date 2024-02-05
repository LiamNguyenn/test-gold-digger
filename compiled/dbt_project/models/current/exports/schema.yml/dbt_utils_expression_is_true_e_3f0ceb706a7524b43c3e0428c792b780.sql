



select
    1
from (select * from "dev"."exports"."exports_braze_users" where user_signin_source = 'career_page') dbt_subquery

where not(user_is_candidate is true)

