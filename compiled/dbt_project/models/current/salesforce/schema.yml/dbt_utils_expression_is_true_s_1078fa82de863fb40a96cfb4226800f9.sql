



select
    1
from (select * from "dev"."salesforce"."salesforce__call_performance_daily" where total_call_duration_in_sec > 0) dbt_subquery

where not(total_calls > 0)

