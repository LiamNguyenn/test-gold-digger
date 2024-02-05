

with ticket_historical_status as (

    select *
    from "dev"."zendesk"."int_zendesk__ticket_historical_status"

), ticket_schedules as (

    select *
    from "dev"."zendesk"."int_zendesk__ticket_schedules"

), schedule as (

    select *
    from "dev"."zendesk"."int_zendesk__schedule_spine"

), ticket_status_crossed_with_schedule as (
  
    select
      ticket_historical_status.ticket_id,
      ticket_historical_status.status as ticket_status,
      ticket_schedules.schedule_id,

      -- take the intersection of the intervals in which the status and the schedule were both active, for calculating the business minutes spent working on the ticket
      greatest(valid_starting_at, schedule_created_at) as status_schedule_start,
      least(valid_ending_at, schedule_invalidated_at) as status_schedule_end,

      -- bringing the following in the determine which schedule (Daylight Savings vs Standard time) to use
      ticket_historical_status.valid_starting_at as status_valid_starting_at,
      ticket_historical_status.valid_ending_at as status_valid_ending_at

    from ticket_historical_status
    left join ticket_schedules
      on ticket_historical_status.ticket_id = ticket_schedules.ticket_id
      -- making sure there is indeed real overlap
      where datediff(
        second,
        greatest(valid_starting_at, schedule_created_at),
        least(valid_ending_at, schedule_invalidated_at)
        ) > 0

), ticket_full_solved_time as (

    select 
      ticket_id,
      ticket_status,
      schedule_id,
      status_schedule_start,
      status_schedule_end,
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
        ticket_status_crossed_with_schedule.status_schedule_start
        )

)
        )

 as date)as timestamp),
        cast(ticket_status_crossed_with_schedule.status_schedule_start as timestamp)
        ) /60
          ) as start_time_in_minutes_from_week,
      (datediff(
        second,
        ticket_status_crossed_with_schedule.status_schedule_start,
        ticket_status_crossed_with_schedule.status_schedule_end
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
        ticket_status_crossed_with_schedule.status_schedule_start
        )

)
        )

 as date) as start_week_date

    from ticket_status_crossed_with_schedule
    group by 1,2,3,4,5,6,7

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
    where floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number -1

), weekly_periods as (

    select

      weeks_cross_ticket_full_solved_time.*,
      -- for each week, at what minute do we start counting?
      cast(greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as integer) as ticket_week_start_time,
      -- for each week, at what minute do we stop counting?
      cast(least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as integer) as ticket_week_end_time
    
    from weeks_cross_ticket_full_solved_time

), intercepted_periods as (
  
    select 
      weekly_periods.ticket_id,
      weekly_periods.week_number,
      weekly_periods.schedule_id,
      weekly_periods.ticket_status,
      weekly_periods.ticket_week_start_time,
      weekly_periods.ticket_week_end_time,
      schedule.start_time_utc as schedule_start_time,
      schedule.end_time_utc as schedule_end_time,
      least(ticket_week_end_time, schedule.end_time_utc) - greatest(weekly_periods.ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
    from weekly_periods
    join schedule on 
      ticket_week_start_time <= schedule.end_time_utc 
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
  
), business_minutes as (
  
    select 
      ticket_id,
      ticket_status,
      case when ticket_status in ('pending') then scheduled_minutes
          else 0 end as agent_wait_time_in_minutes,
      case when ticket_status in ('new', 'open', 'hold') then scheduled_minutes
          else 0 end as requester_wait_time_in_minutes,
      case when ticket_status in ('new', 'open', 'hold', 'pending') then scheduled_minutes
          else 0 end as solve_time_in_minutes,
      case when ticket_status in ('new', 'open') then scheduled_minutes
          else 0 end as agent_work_time_in_minutes,
      case when ticket_status in ('hold') then scheduled_minutes
          else 0 end as on_hold_time_in_minutes,
      case when ticket_status = 'new' then scheduled_minutes
          else 0 end as new_status_duration_minutes,
      case when ticket_status = 'open' then scheduled_minutes
          else 0 end as open_status_duration_minutes
    from intercepted_periods

)
  
    select 
      ticket_id,
      sum(agent_wait_time_in_minutes) as agent_wait_time_in_business_minutes,
      sum(requester_wait_time_in_minutes) as requester_wait_time_in_business_minutes,
      sum(solve_time_in_minutes) as solve_time_in_business_minutes,
      sum(agent_work_time_in_minutes) as agent_work_time_in_business_minutes,
      sum(on_hold_time_in_minutes) as on_hold_time_in_business_minutes,
      sum(new_status_duration_minutes) as new_status_duration_in_business_minutes,
      sum(open_status_duration_minutes) as open_status_duration_in_business_minutes
    from business_minutes
    group by 1