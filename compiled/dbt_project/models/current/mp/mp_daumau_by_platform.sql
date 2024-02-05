

with dates as (
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
    where generated_number <= 180
    order by generated_number

)
        where "date" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")

        and date > (select max(date) from "dev"."mp"."daumau_by_platform" )
   
)
                               
, platform_events as (
        select
          user_id, user_email, "timestamp", platform
        from
          "dev"."customers"."events" 
        where app_version_string is not null and platform != ''         
        and "timestamp" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
      )
, platform_dau as (
        select
          DATE_TRUNC('day', timestamp) as "date"
          , platform          
          , count(distinct coalesce(user_id, user_email)) as daily_users
        from
          platform_events         
        group by
          1, 2
      )
      , platform_mau as (
        select
          dates.date
          , e.platform        
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join platform_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1, 2
      )

select
      m.date
      , m.platform	
      , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      platform_mau m
      left join platform_dau d on
        m.date = d.date                   
        and m.platform = d.platform