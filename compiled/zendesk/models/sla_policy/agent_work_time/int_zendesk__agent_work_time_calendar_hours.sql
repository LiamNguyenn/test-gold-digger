-- Calculate breach time for agent work time, calendar hours
with agent_work_time_filtered_statuses as (

  select *
  from "dev"."zendesk"."int_zendesk__agent_work_time_filtered_statuses"
  where not in_business_hours

), agent_work_time_calendar_minutes as (

  select 
    *,
    datediff(
        minute,
        valid_starting_at,
        valid_ending_at
        ) as calendar_minutes,
    sum(datediff(
        minute,
        valid_starting_at,
        valid_ending_at
        ) ) 
      over (partition by ticket_id, sla_applied_at order by valid_starting_at rows between unbounded preceding and current row) as running_total_calendar_minutes
  from agent_work_time_filtered_statuses

), agent_work_time_calendar_minutes_flagged as (

select 
  agent_work_time_calendar_minutes.*,
  target - running_total_calendar_minutes as remaining_target_minutes,
  case when (target - running_total_calendar_minutes) < 0 
      and 
        (lag(target - running_total_calendar_minutes) over
        (partition by ticket_id, sla_applied_at order by valid_starting_at) >= 0 
        or 
        lag(target - running_total_calendar_minutes) over
        (partition by ticket_id, sla_applied_at order by valid_starting_at) is null) 
        then true else false end as is_breached_during_schedule
        
from  agent_work_time_calendar_minutes

), final as (
  select
    *,
    (remaining_target_minutes + calendar_minutes) as breach_minutes,
    

        dateadd(
        minute,
        (remaining_target_minutes + calendar_minutes),
        valid_starting_at
        )

 as sla_breach_at
  from agent_work_time_calendar_minutes_flagged

)

select *
from final