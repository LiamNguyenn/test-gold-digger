--REPLY TIME SLA
-- step 2, figure out when the sla will breach for sla's in calendar hours. The calculation is relatively straightforward.

with sla_policy_applied as (

  select *
  from "dev"."zendesk"."int_zendesk__sla_policy_applied"

), final as (
  select
    *,
    

        dateadd(
        minute,
        cast(target as integer ),
        sla_applied_at
        )

 as sla_breach_at
  from sla_policy_applied
  where not in_business_hours
    and metric in ('next_reply_time', 'first_reply_time')

)

select *
from final