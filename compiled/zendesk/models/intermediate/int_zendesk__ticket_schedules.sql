

with ticket as (
  
  select *
  from "dev"."zendesk"."stg_zendesk__ticket"

), ticket_schedule as (
 
  select *
  from "dev"."zendesk"."stg_zendesk__ticket_schedule"

), schedule as (
 
  select *
  from "dev"."zendesk"."stg_zendesk__schedule"


), default_schedule_events as (
-- Goal: understand the working schedules applied to tickets, so that we can then determine the applicable business hours/schedule.
-- Your default schedule is used for all tickets, unless you set up a trigger to apply a specific schedule to specific tickets.

-- This portion of the query creates ticket_schedules for these "default" schedules, as the ticket_schedule table only includes
-- trigger schedules



    

    

    

  select
    ticket.ticket_id,
    ticket.created_at as schedule_created_at,
    '7089' as schedule_id
  from ticket
  left join ticket_schedule as first_schedule
    on first_schedule.ticket_id = ticket.ticket_id
    and 

        dateadd(
        second,
        -5,
        first_schedule.created_at
        )

 <= ticket.created_at
    and first_schedule.created_at >= ticket.created_at    
  where first_schedule.ticket_id is null

), schedule_events as (
  
  select
    *
  from default_schedule_events
  
  union all
  
  select 
    ticket_id,
    created_at as schedule_created_at,
    schedule_id
  from ticket_schedule

), ticket_schedules as (
  
  select 
    ticket_id,
    schedule_id,
    schedule_created_at,
    coalesce(lead(schedule_created_at) over (partition by ticket_id order by schedule_created_at)
            , 

        dateadd(
        hour,
        1000,
        getdate()
        )

 ) as schedule_invalidated_at
  from schedule_events

)
select
  *
from ticket_schedules