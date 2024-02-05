

-- REQUESTER WAIT TIME
-- This is complicated, as SLAs minutes are only counted while the ticket is in 'new', 'open', and 'on-hold' status.

-- Additionally, for business hours, only 'new', 'open', and 'on-hold' status hours are counted if they are also during business hours
with requester_wait_time_filtered_statuses as (

  select *
  from "dev"."zendesk"."int_zendesk__requester_wait_time_filtered_statuses"
  where in_business_hours

), schedule as (

  select * 
  from "dev"."zendesk"."int_zendesk__schedule_spine"

), ticket_schedules as (

  select * 
  from "dev"."zendesk"."int_zendesk__ticket_schedules"
  
-- cross schedules with work time
), ticket_status_crossed_with_schedule as (
  
    select
      requester_wait_time_filtered_statuses.ticket_id,
      requester_wait_time_filtered_statuses.sla_applied_at,
      requester_wait_time_filtered_statuses.target,
      requester_wait_time_filtered_statuses.sla_policy_name,
      ticket_schedules.schedule_id,

      -- take the intersection of the intervals in which the status and the schedule were both active, for calculating the business minutes spent working on the ticket
      greatest(valid_starting_at, schedule_created_at) as valid_starting_at,
      least(valid_ending_at, schedule_invalidated_at) as valid_ending_at,

      -- bringing the following in the determine which schedule (Daylight Savings vs Standard time) to use
      valid_starting_at as status_valid_starting_at,
      valid_ending_at as status_valid_ending_at

    from requester_wait_time_filtered_statuses
    left join ticket_schedules
      on requester_wait_time_filtered_statuses.ticket_id = ticket_schedules.ticket_id
    where datediff(
        second,
        greatest(valid_starting_at, schedule_created_at),
        least(valid_ending_at, schedule_invalidated_at)
        ) > 0

), ticket_full_solved_time as (

    select 
      ticket_id,
      sla_applied_at,
      target,
      sla_policy_name,
      schedule_id,
      valid_starting_at,
      valid_ending_at,
      status_valid_starting_at,
      status_valid_ending_at,
      (datediff(
        second,
        cast(-- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        ticket_status_crossed_with_schedule.valid_starting_at
        )

)
        )

 as date)as timestamp),
        cast(ticket_status_crossed_with_schedule.valid_starting_at as timestamp)
        ) /60
          ) as valid_starting_at_in_minutes_from_week,
      (datediff(
        second,
        ticket_status_crossed_with_schedule.valid_starting_at,
        ticket_status_crossed_with_schedule.valid_ending_at
        ) /60
            ) as raw_delta_in_minutes,
    -- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        ticket_status_crossed_with_schedule.valid_starting_at
        )

)
        )

 as date) as start_week_date

    from ticket_status_crossed_with_schedule
    group by 1,2,3,4,5,6,7,8,9,10

), weeks as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
    
    

    )

    select *
    from unioned
    where generated_number <= 208
    order by generated_number



), weeks_cross_ticket_full_solved_time as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select 
      ticket_full_solved_time.*,
      cast(generated_number - 1 as integer) as week_number
    from ticket_full_solved_time
    cross join weeks
    where floor((valid_starting_at_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number -1

), weekly_period_requester_wait_time as (

    select 

      ticket_id,
      sla_applied_at,
      valid_starting_at,
      valid_ending_at,
      status_valid_starting_at,
      status_valid_ending_at,
      target,
      sla_policy_name,
      valid_starting_at_in_minutes_from_week,
      raw_delta_in_minutes,
      week_number,
      schedule_id,
      start_week_date,
      cast(greatest(0, valid_starting_at_in_minutes_from_week - week_number * (7*24*60)) as integer) as ticket_week_start_time_minute,
      cast(least(valid_starting_at_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as integer) as ticket_week_end_time_minute
    
    from weeks_cross_ticket_full_solved_time

), intercepted_periods_agent as (
  
    select 
      weekly_period_requester_wait_time.ticket_id,
      weekly_period_requester_wait_time.sla_applied_at,
      weekly_period_requester_wait_time.target,
      weekly_period_requester_wait_time.sla_policy_name,
      weekly_period_requester_wait_time.valid_starting_at,
      weekly_period_requester_wait_time.valid_ending_at,
      weekly_period_requester_wait_time.week_number,
      weekly_period_requester_wait_time.ticket_week_start_time_minute,
      weekly_period_requester_wait_time.ticket_week_end_time_minute,
      schedule.start_time_utc as schedule_start_time,
      schedule.end_time_utc as schedule_end_time,
      least(ticket_week_end_time_minute, schedule.end_time_utc) - greatest(weekly_period_requester_wait_time.ticket_week_start_time_minute, schedule.start_time_utc) as scheduled_minutes
    from weekly_period_requester_wait_time
    join schedule on ticket_week_start_time_minute <= schedule.end_time_utc 
      and ticket_week_end_time_minute >= schedule.start_time_utc
      and weekly_period_requester_wait_time.schedule_id = schedule.schedule_id
      -- this chooses the Daylight Savings Time or Standard Time version of the schedule
      -- We have everything calculated within a week, so take us to the appropriate week first by adding the week_number * minutes-in-a-week to the minute-mark where we start and stop counting for the week
      and cast( 

    dateadd(
        minute,
        week_number * (7*24*60) + ticket_week_end_time_minute,
        start_week_date
        )

 as timestamp) > cast(schedule.valid_from as timestamp)
      and cast( 

    dateadd(
        minute,
        week_number * (7*24*60) + ticket_week_start_time_minute,
        start_week_date
        )

 as timestamp) < cast(schedule.valid_until as timestamp)
  
), intercepted_periods_with_running_total as (
  
    select 
      *,
      sum(scheduled_minutes) over 
        (partition by ticket_id, sla_applied_at 
          order by valid_starting_at, week_number, schedule_end_time
          rows between unbounded preceding and current row)
        as running_total_scheduled_minutes

    from intercepted_periods_agent


), intercepted_periods_agent_with_breach_flag as (
  select 
    intercepted_periods_with_running_total.*,
    target - running_total_scheduled_minutes as remaining_target_minutes,
    lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time) as lag_check,
    case when (target - running_total_scheduled_minutes) = 0 then true
       when (target - running_total_scheduled_minutes) < 0 
        and 
          (lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time) > 0 
          or 
          lag(target - running_total_scheduled_minutes) over
          (partition by ticket_id, sla_applied_at order by valid_starting_at, week_number, schedule_end_time) is null) 
          then true else false end as is_breached_during_schedule
          
  from  intercepted_periods_with_running_total

), intercepted_periods_agent_filtered as (

  select
    *,
    (remaining_target_minutes + scheduled_minutes) as breach_minutes,
    greatest(ticket_week_start_time_minute, schedule_start_time) + (remaining_target_minutes + scheduled_minutes) as breach_minutes_from_week
  from intercepted_periods_agent_with_breach_flag

), requester_wait_business_breach as (
  
  select 
    *,
    

        dateadd(
        minute,
        cast(((7*24*60) * week_number) + breach_minutes_from_week as integer ),
        date_trunc('week', valid_starting_at)
        )

 as sla_breach_at
  from intercepted_periods_agent_filtered

)

select * 
from requester_wait_business_breach