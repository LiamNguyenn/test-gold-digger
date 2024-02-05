

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

        and "date" > (SELECT MAX("date") FROM "dev"."mp"."daumau_by_user_app_type" ) 
   
)
                               
, user_app_events as (
        select
          e.user_id, 
          e.user_email,
  			e.user_type, 
  			case when app_version_string is not null then 'mobile' else 'web' end as app_type, 
  		 e.timestamp
        from
          "dev"."customers"."events" as e          
        where          
          e.timestamp < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
      )
      , user_app_dau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"
          , e.user_type
          , e.app_type    	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e
        where  
        e.user_type != ''
        and e.app_type != ''
        group by
          1, 2,3
        
union
		select
          DATE_TRUNC('day', e.timestamp) as "date"
          , 'any' as user_type
          , e.app_type       	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e
        where        
        e.app_type != ''
        group by
          1, 3
        
union
		select
          DATE_TRUNC('day', e.timestamp) as "date"
          , e.user_type
          , 'any' as app_type     	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e
         where        
        e.user_type != ''        
        group by
          1, 2
        
union
		select
          DATE_TRUNC('day', e.timestamp) as "date"
          , 'any' as user_type
          , 'any' as app_type	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e        
        group by
          1
      )
      , user_app_mau as (
        select
          dates.date
          , e.user_type
          , e.app_type
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)                
        where        
        e.user_type != ''
        and e.app_type != ''        
        group by
          1, 2,3
union
       select
          dates.date
          , 'any' as user_type
          , e.app_type   
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
                where       
        e.app_type != ''      
        group by
          1,3
        
union
       select
          dates.date
          , e.user_type
          , 'any' as app_type  
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
                where        
        e.user_type != ''        
        group by
          1,2
        
union
       select
          dates.date
          , 'any' as user_type
          , 'any' as app_type     
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1
      )

select
      m.date
	, m.user_type
	, m.app_type 
      , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      user_app_mau m
      left join user_app_dau d on
        m.date = d.date and m.user_type = d.user_type and m.app_type = d.app_type