

with ticket_resolution_times_calendar as (

    select *
    from "dev"."zendesk"."int_zendesk__ticket_resolution_times_calendar"

), ticket_schedules as (

    select *
    from "dev"."zendesk"."int_zendesk__ticket_schedules"

), schedule as (

    select *
    from "dev"."zendesk"."int_zendesk__schedule_spine"

), ticket_first_resolution_time as (

  select 
    ticket_resolution_times_calendar.ticket_id,
    ticket_schedules.schedule_created_at,
    ticket_schedules.schedule_invalidated_at,
    ticket_schedules.schedule_id,

    -- bringing this in the determine which schedule (Daylight Savings vs Standard time) to use
    min(ticket_resolution_times_calendar.first_solved_at) as first_solved_at,

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
        ticket_schedules.schedule_created_at
        )

)
        )

 as date)as timestamp),
        cast(ticket_schedules.schedule_created_at as timestamp)
        ) /60
          ) as start_time_in_minutes_from_week,
    greatest(0,
      (
        datediff(
        second,
        ticket_schedules.schedule_created_at,
        least(ticket_schedules.schedule_invalidated_at, min(ticket_resolution_times_calendar.first_solved_at))
        )/60
        )) as raw_delta_in_minutes,
    -- Sunday as week start date
cast(

    dateadd(
        day,
        -1,
        date_trunc('week', 

    dateadd(
        day,
        1,
        ticket_schedules.schedule_created_at
        )

)
        )

 as date) as start_week_date
      
  from ticket_resolution_times_calendar
  join ticket_schedules on ticket_resolution_times_calendar.ticket_id = ticket_schedules.ticket_id
  group by 1, 2, 3, 4

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



), weeks_cross_ticket_first_resolution_time as (
    -- because time is reported in minutes since the beginning of the week, we have to split up time spent on the ticket into calendar weeks
    select 

      ticket_first_resolution_time.*,
      cast(generated_number - 1 as integer) as week_number

    from ticket_first_resolution_time
    cross join weeks
    where floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)) >= generated_number - 1


), weekly_periods as (
  
    select 

      weeks_cross_ticket_first_resolution_time.*,
      cast(greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as integer) as ticket_week_start_time,
      cast(least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as integer) as ticket_week_end_time
    
    from weeks_cross_ticket_first_resolution_time

), intercepted_periods as (

  select ticket_id,
         week_number,
         weekly_periods.schedule_id,
         ticket_week_start_time,
         ticket_week_end_time,
         schedule.start_time_utc as schedule_start_time,
         schedule.end_time_utc as schedule_end_time,
         least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
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

)

  select 
    ticket_id,
    sum(scheduled_minutes) as first_resolution_business_minutes
  from intercepted_periods
  group by 1