


with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) as "date"
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
          where "date" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")

        and "date" > (SELECT MAX("date") FROM "dev"."mp"."daumau" ) 
          
)                               
      , dau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          "dev"."customers"."events" as e          
        where          
        e.timestamp < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")

          and e.timestamp > dateadd(day, 1, (select max("date") from "dev"."mp"."daumau")) 
             
        group by
          1
      )
      , mau as (
        select
          dates.date
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates join "dev"."customers"."events" as e on e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)          
        where          
          e.timestamp < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
        group by
          1
      )

select
      m.date      
      , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      mau m
      left join dau d on
        m.date = d.date