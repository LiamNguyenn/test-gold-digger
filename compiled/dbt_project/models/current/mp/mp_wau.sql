

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

        and "date" > (SELECT MAX("date") FROM "dev"."mp"."wau" ) 
        

)                               
     , wau as (
        select
          dates.date
          , count(distinct e.user_id) as weekly_users
        from
          dates join "dev"."customers"."events" as e on e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -6, dates.date)
          join "dev"."postgres_public"."users" u on
            u.uuid = e.user_id
          join "dev"."postgres_public"."members" m on
            m.user_id = u.id
        where
          u.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
          and not u._fivetran_deleted
          and not u.is_shadow_data
          and not m.system_manager
          and not m."system_user"
          --and not m.independent_contractor
          and not m._fivetran_deleted
          and not m.is_shadow_data
          and e."timestamp" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")          
        group by
          1
      )    
select
      w.date      
      , weekly_users      
    from
      wau w