

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
     + 
    
    p9.generated_number * power(2, 9)
    
    
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
    
    

    )

    select *
    from unioned
    where generated_number <= 600
    order by generated_number

)
          where "date" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
          
          and date > (select max(date) from  "dev"."mp"."mdaumau_by_family_app_type")     
             
)       
  , module_events as (
    select
      e.user_id
        , e.timestamp
        , e.module
        , mo.product_family
        , case when app_version_string is not null then 'mobile' else 'web' end as app_type
    from
      "dev"."customers"."events" as e
      join "dev"."customers"."users" us on 
          e.user_id = us.user_uuid
      join "dev"."customers"."accounts" a on 
          us.account_list = a.external_id
          and a.account_stage != 'Churned'
      join "dev"."eh_product"."module_ownership" mo on
          mo.event_module = e.module
          and mo.product_family is not null    
    where "timestamp" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
      and e.module != 'mobile'
      and e.module != 'others'
      and e.module != 'Sign In'
  )
  , product_family_app_type_dau as (
    select
      DATE_TRUNC('day', e.timestamp) as "date"
      , e.app_type
      , e.product_family
      , count(distinct e.user_id) as daily_users
    from
      module_events as e
    group by 1,2,3
  )
  , product_family_app_type_mau as (
    select
      dates.date
      , e.app_type
      , e.product_family
      , count(distinct e.user_id) as monthly_users
    from
      dates
      join module_events as e on
        e.timestamp < dateadd(day, 1, dates.date)
        and e.timestamp > dateadd(day, -29, dates.date)
    group by 1,2,3
  )

select
  m.date
  , m.app_type
  , m.product_family
  , coalesce(daily_users, 0) as daily_users
  , monthly_users
  , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
from
  product_family_app_type_mau m
  left join product_family_app_type_dau d on
    m.date = d.date
    and m.product_family = d.product_family
    and m.app_type = d.app_type