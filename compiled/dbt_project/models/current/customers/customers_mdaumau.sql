

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

        and "date" > (select max("date") from "dev"."customers"."mdaumau")

)
                               
, events as (
        select
          e.user_id, e.timestamp
        from
          "dev"."customers"."events" as e 
  		  join "dev"."customers"."users" u on e.user_id = u.user_uuid
		join "dev"."customers"."accounts" a on u.account_list = a.external_id
        where a.account_stage != 'Churned'
        and e.timestamp < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
      )
      , mdau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"
          , count(distinct e.user_id) as daily_users
        from
          events as e   
             
        where e.timestamp > dateadd('day', 1, (select max("date") from "dev"."customers"."mdaumau"))
        
        group by
          1
      )
      , mmau as (
        select
          dates.date
          , count(distinct e.user_id) as monthly_users
        from
          dates
          join events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1
      )

select
      m.date
      , coalesce(daily_users, 0) as mdau
      , monthly_users as mmau
      , coalesce(mdau, 0) / mmau :: float as mdau_mau
    from
     mmau m
      left join mdau d on
        m.date = d.date