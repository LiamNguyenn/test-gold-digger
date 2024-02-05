

with 
  dates as (
    select
      DATEADD('day', -generated_number::int, (current_date + 1)) date
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
    
    

    )

    select *
    from unioned
    where generated_number <= 365
    order by generated_number

)
    where
        "date" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
        
            and "date" > (SELECT MAX("date") FROM "dev"."mp"."swag_daumau_country" ) 
           
  )
  , user_country as (
    select
        mu.eh_platform_user_id
        ,u.uuid
        ,coalesce(mu.country, mu.eh_platform_employment_location) as country
    from 
        "dev"."marketing"."users_from_snapshot" mu
        left join "dev"."postgres_public"."users" u on
            mu.eh_platform_user_id = u.id
  )
  , swag_events as ( 
    select e.user_id, e.user_email, e.timestamp, uc.country
    from 
      "dev"."customers"."events" e
      left join user_country uc on
        e.user_id = uc.uuid
    where 
      e.app_version_string is not null
      and e.timestamp < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
    
  )
  , swag_country_dau as (
    select
      DATE_TRUNC('day', e.timestamp) as "date"
      , e.country
      , count(distinct e.user_email) as daily_users
    from
      swag_events as e
    group by 1,2
  )
  , swag_country_mau as (
    select
      dates.date
      , e.country
      , count(distinct e.user_email) as monthly_users
    from
      dates
      join swag_events as e on
        e.timestamp < dateadd(day, 1, dates.date)
        and e.timestamp > dateadd(day, -29, dates.date)
    group by 1,2
  )

select
  m.date
  , m.country
  , coalesce(daily_users, 0) as daily_users
  , monthly_users
  , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
from
  swag_country_mau m
  left join swag_country_dau d on
    m.date = d.date
    and m.country = d.country