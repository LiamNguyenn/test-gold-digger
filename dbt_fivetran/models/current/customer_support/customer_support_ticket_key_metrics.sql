with
  ticket_master_table as (
    select distinct
      t.custom_country as country
      , t.id as ticket_id
      , g.name as group_name
      , date(t.updated_at) updated_date
      , t.custom_total_time_spent_sec_ / 60 as time_spent_min
      , date(t.created_at) as created_at
      , u.name as assignee
      , tfh.value status
    from
      {{ source('zendesk', 'ticket') }} t
      inner join {{ source('zendesk', 'group') }} g on
        t.group_id = g.id
        and g.name in (
            'Support Partners AU'
,'Support Partners NZ'
,'Support Partners UK'
,'Support Partners SEA'
,'Support Swag AU'
,'Support Swag NZ'
,'Support Swag Partners NZ'
,'Support Swag Partners AU'
,'Support Swag UK'
,'Support Swag Partners UK'
,'Support Swag SEA'
,'Support Swag Partners SEA'
,'Support'
,'Support HR AU'
,'Support HR NZ'
,'Support HR SEA'
,'Support Payroll SEA'
,'Support Payroll NZ'
,'Support Payroll AU'
,'Support Payroll Escalation'
,'Support HR UK'
,'Support Payroll UK'
        )
      left join {{ source('zendesk', 'ticket_field_history') }} tfh on
        t.id = tfh.ticket_id
        and tfh.field_name ilike '%status%'
      left join {{ source('zendesk', 'user') }} as u on
      t.assignee_id = u.id
  where
    t.via_channel != 'side_conversation'
    and t.created_at >= '2019-01-01'
  )
  , dim_date as (
    select distinct
      DATEADD('day', -generated_number::int, current_date) as "date"
    from ({{ dbt_utils.generate_series(upper_bound=3000) }})
  )
select distinct
  cast(dim_date.date as date) date
  , (case when avg_t.group_name is null then 'untracked' else avg_t.group_name end) as group_name
  , (case 
          when avg_t.country = 'au' then 'Australia'
          when avg_t.country = 'uk' then 'United Kingdom'
          when avg_t.country = 'nz' then 'New Zealand' 
           when avg_t.country = 'my' then 'Malaysia' 
          when avg_t.country = 'sg' then 'Singapore'  
          else 'untracked' end) as country
  , sum(avg_t.ticket_count) total_tickets_assignees
  , count (distinct avg_t.assignee) total_assignees
  , (case when count(avg_t.assignee) >0 then sum(avg_t.ticket_count)/count (distinct avg_t.assignee) end) avg_ticket_per_agent 
  , sum(aht.total_tickets) total_tickets_time
  , sum(aht.time_spent_min) as total_time_spent_min
  , sum(aht.resolution_time_day) as total_resolution_time_day
from
  dim_date
  -- avg tickets per agent
  left join (
    select
      cast(created_at as date) as date
      , group_name
      , country
      , assignee
      , count(ticket_id) ticket_count
    from
      ticket_master_table
    group by
      1
      , 2
      , 3
      , 4
  )
  avg_t on
    avg_t.date = dim_date.date
    -- avg handling time
  left join (
    select
      updated_date as date
      , group_name
      , country
      , sum(time_spent_min) time_spent_min
      , sum(updated_date - created_at) resolution_time_day
      , count( distinct ticket_id) total_tickets
    from
      ticket_master_table
    where
      status in (
        'solved'
      )
      group by 1,2,3
  )
  aht on
    dim_date.date = aht.date
    and avg_t.country = aht.country
    and avg_t.group_name = aht.group_name
group by
  1
  , 2
  , 3