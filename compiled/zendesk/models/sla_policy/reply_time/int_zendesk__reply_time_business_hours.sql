

-- step 3, determine when an SLA will breach for SLAs that are in business hours

with ticket_schedules as (

  select *
  from "dev"."zendesk"."int_zendesk__ticket_schedules"

), schedule as (

  select *
  from "dev"."zendesk"."int_zendesk__schedule_spine"

), sla_policy_applied as (

  select *
  from "dev"."zendesk"."int_zendesk__sla_policy_applied"


), schedule_business_hours as (

  select 
    schedule_id,
    sum(end_time - start_time) as total_schedule_weekly_business_minutes
  -- referring to stg_zendesk__schedule instead of int_zendesk__schedule_spine just to calculate total minutes
  from "dev"."zendesk"."stg_zendesk__schedule"
  group by 1

), ticket_sla_applied_with_schedules as (

  select 
    sla_policy_applied.*,
    ticket_schedules.schedule_id,
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
        sla_policy_applied.sla_applied_at
        )

)
        )

 as date)as timestamp),
        cast(sla_policy_applied.sla_applied_at as timestamp)
        ) /60
          ) as start_time_in_minutes_from_week,
      schedule_business_hours.total_schedule_weekly_business_minutes,
    -- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        sla_policy_applied.sla_applied_at
        )

)
        )

 as date) as start_week_date

  from sla_policy_applied
  left join ticket_schedules on sla_policy_applied.ticket_id = ticket_schedules.ticket_id
    and 

        dateadd(
        second,
        -1,
        ticket_schedules.schedule_created_at
        )

 <= sla_policy_applied.sla_applied_at
    and 

        dateadd(
        second,
        -1,
        ticket_schedules.schedule_invalidated_at
        )

 > sla_policy_applied.sla_applied_at
  left join schedule_business_hours 
    on ticket_schedules.schedule_id = schedule_business_hours.schedule_id
  where sla_policy_applied.in_business_hours
    and metric in ('next_reply_time', 'first_reply_time')
  
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



), weeks_cross_ticket_sla_applied as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select 

      ticket_sla_applied_with_schedules.*,
      cast(generated_number - 1 as integer) as week_number

    from ticket_sla_applied_with_schedules
    cross join weeks
    where 
    ceiling(target/total_schedule_weekly_business_minutes)

 >= generated_number - 1

), weekly_periods as (
  
  select 
    weeks_cross_ticket_sla_applied.*,
    cast(greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as integer) as ticket_week_start_time,
    cast((7*24*60) as integer) as ticket_week_end_time
  from weeks_cross_ticket_sla_applied

), intercepted_periods as (

  select 
    weekly_periods.*,
    schedule.start_time_utc as schedule_start_time,
    schedule.end_time_utc as schedule_end_time,
    (schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) as lapsed_business_minutes,
    sum(schedule.end_time_utc - greatest(ticket_week_start_time,schedule.start_time_utc)) over 
      (partition by ticket_id, metric, sla_applied_at 
        order by week_number, schedule.start_time_utc
        rows between unbounded preceding and current row) as sum_lapsed_business_minutes
  from weekly_periods
  join schedule on ticket_week_start_time <= schedule.end_time_utc 
    and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.schedule_id
    -- this chooses the Daylight Savings Time or Standard Time version of the schedule
    -- We have everything calculated within a week, so take us to the appropriate week first by adding the week_number * minutes-in-a-week to the minute-mark where we start and stop counting for the week
    and cast( 

    dateadd(
        minute,
        week_number * (7*24*60) + ticket_week_end_time,
        start_week_date
        )

 as timestamp) > cast(schedule.valid_from as timestamp)
    and cast( 

    dateadd(
        minute,
        week_number * (7*24*60) + ticket_week_start_time,
        start_week_date
        )

 as timestamp) < cast(schedule.valid_until as timestamp)

), intercepted_periods_with_breach_flag as (
  
  select 
    *,
    target - sum_lapsed_business_minutes as remaining_minutes,
    case when (target - sum_lapsed_business_minutes) < 0 
      and 
        (lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, metric, sla_applied_at order by week_number, schedule_start_time) >= 0 
        or 
        lag(target - sum_lapsed_business_minutes) over
        (partition by ticket_id, metric, sla_applied_at order by week_number, schedule_start_time) is null) 
        then true else false end as is_breached_during_schedule -- this flags the scheduled period on which the breach took place
  from intercepted_periods

), intercepted_periods_with_breach_flag_calculated as (

  select
    *,
    schedule_end_time + remaining_minutes as breached_at_minutes,
    date_trunc('week', sla_applied_at) as starting_point,
    

        dateadd(
        minute,
        cast(((7*24*60) * week_number) + (schedule_end_time + remaining_minutes) as integer ),
        cast(-- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        sla_applied_at
        )

)
        )

 as date) as timestamp)
        )

 as sla_breach_at,
    

        dateadd(
        minute,
        cast(((7*24*60) * week_number) + (schedule_start_time) as integer ),
        cast(-- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        sla_applied_at
        )

)
        )

 as date) as timestamp)
        )

 as sla_schedule_start_at,
    

        dateadd(
        minute,
        cast(((7*24*60) * week_number) + (schedule_end_time) as integer ),
        cast(-- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        sla_applied_at
        )

)
        )

 as date) as timestamp)
        )

 as sla_schedule_end_at,
    cast(

    dateadd(
        day,
        6,
        -- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        sla_applied_at
        )

)
        )

 as date)
        )

 as date) as week_end_date
  from intercepted_periods_with_breach_flag

), reply_time_business_hours_sla as (

  select
    ticket_id,
    sla_policy_name,
    metric,
    ticket_created_at,
    sla_applied_at,
    greatest(sla_applied_at,sla_schedule_start_at) as sla_schedule_start_at,
    sla_schedule_end_at,
    target,
    sum_lapsed_business_minutes,
    in_business_hours,
    sla_breach_at,
    is_breached_during_schedule
  from intercepted_periods_with_breach_flag_calculated

) 

select * 
from reply_time_business_hours_sla