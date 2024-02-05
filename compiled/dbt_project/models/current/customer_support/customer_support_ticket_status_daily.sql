with
  dates as (
    select distinct
      DATEADD('day', -generated_number::int, current_date) as "date"
    from (

    

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
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
    
    
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
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 3000
    order by generated_number

)
  )
  , ticket_status_history as (
    select
      th.ticket_id
      , t.custom_country as country
      , g.name as group_name
      , th.updated::date as agg_date
      , "value"
      , row_number() over(partition by th.ticket_id, th.updated::date order by updated desc) as rn
    from
      "dev"."zendesk"."ticket_field_history" th
      inner join "dev"."zendesk"."ticket" t on
        th.ticket_id = t.id
        and t.created_at >= '2019-01-01'
        and t.via_channel != 'side_conversation'
      inner join "dev"."zendesk"."group" g on
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
    where
      th.field_name = 'status'
  )
  , min_max_ticket as (
    select
      ticket_id
      , min(agg_date)
    from
      ticket_status_history
    group by
      1
  )
  , ticket_over_time_w_status as (
    select
      *
      , last_value(country ignore nulls) over(partition by ticket_id order by date rows unbounded preceding) as country_c
      , last_value(group_name ignore nulls) over(partition by ticket_id order by date rows unbounded preceding) as group_name_c 
      , last_value(value ignore nulls) over(partition by ticket_id order by date rows unbounded preceding) as status
    from
      (
        select
          mmt.ticket_id
          , d.*
          , tsh.value
          , tsh.country
          , tsh.group_name
        from
          dates d
          join min_max_ticket mmt on
            d.date >= mmt.min
            and d.date <= current_date
          left join ticket_status_history tsh on
            d.date = agg_date
            and mmt.ticket_id = tsh.ticket_id
            and rn = 1
        order by
          1 asc
      )
  )
select
  cast(date as date) date
  , (
    case
      when country_c is null
        then 'untracked'
      else country_c
    end
  )
  as country
  , (
    case
      when group_name_c is null
        then 'untracked'
      else group_name_c
    end
  )
  as group_name
  , count(
    case
      when status = 'open'
        then ticket_id
      else null
    end
  )
  as open_tickets
  , count(
    case
      when status = 'closed'
        then ticket_id
      else null
    end
  )
  as closed_tickets
  , count(
    case
      when status = 'hold'
        then ticket_id
      else null
    end
  )
  as hold_tickets
  , count(
    case
      when status = 'solved'
        then ticket_id
      else null
    end
  )
  as solved_tickets
  , count(
    case
      when status = 'new'
        then ticket_id
      else null
    end
  )
  as new_tickets
  , count(
    case
      when status = 'pending'
        then ticket_id
      else null
    end
  )
  as pending_tickets
  , count(
    case
      when status = 'deleted'
        then ticket_id
      else null
    end
  )
  as deleted_tickets
  , count(
    case
      when status is null
        then ticket_id
      else null
    end
  )
  as null_tickets
from
  ticket_over_time_w_status
where
  date >= cast('2019-01-01' as date)
group by
  1
  , 2
  , 3
order by
  date desc