

with dates as (
    select
          DATEADD('day', -generated_number::int, (current_date + 1)) as date
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
    
    

    )

    select *
    from unioned
    where generated_number <= 90
    order by generated_number

)
          where "date" < current_date
          
            and date > (select max(date) from "dev"."tableau"."companydash_registered_users" )
          
            
)
, employees as (
    select user_uuid
      , ( case
          when lower(work_country) = 'au'
            then 'Australia'
          when lower(work_country) = 'gb'
            then 'United Kingdom'
          when lower(work_country) = 'sg'
            then 'Singapore'
          when lower(work_country) = 'my'
            then 'Malaysia'
          when lower(work_country) = 'nz'
            then 'New Zealand'
          else 'untracked'
        end
      )
      as country
      , min(created_at::date) as first_registered_date

    from "dev"."employment_hero"."employees"

    group by 1, 2
)
, app_downloads as (
    select ee.user_uuid
        , ( case
          when lower(ee.work_country) = 'au'
            then 'Australia'
          when lower(ee.work_country) = 'gb'
            then 'United Kingdom'
          when lower(ee.work_country) = 'sg'
            then 'Singapore'
          when lower(ee.work_country) = 'my'
            then 'Malaysia'
          when lower(ee.work_country) = 'nz'
            then 'New Zealand'
          else 'untracked'
        end
      )
      as country
        , min(timestamp::date) as first_app_signin_date

    from "dev"."customers"."events" ce

    inner join "dev"."employment_hero"."employees" ee
      on ee.uuid = ce.member_uuid

    where name ~~* '%sign in%'
      and platform ~~* '%mobile%'
    
    group by 1, 2)

, registered_users as (
    select first_registered_date
      , country
      , count(distinct user_uuid) as registered_users

    from employees

    group by 1, 2
)

, registered_app_users as (
    select first_app_signin_date
      , country
      , count(distinct user_uuid) as registered_app_users

    from app_downloads

    group by 1, 2
)
, cumulative_users as (
    select d.date
        , country
        , sum(registered_users) as total_registered_users

    from dates d

    left join registered_users e
        on d.date >= e.first_registered_date

    group by 1, 2

    order by 1
)
, cumulative_app_users as (
    select d.date
        , country
        , sum(registered_app_users) as total_registered_app_users

    from dates d

    left join registered_app_users a
        on d.date >= a.first_app_signin_date

    group by 1, 2

    order by 1
)
, total as (
  select "date"
    , country
    , 'Total' as type
    , total_registered_users::int as users

  from cumulative_users

  union 

  select "date"
    , country
    , 'App' as type
    , total_registered_app_users::int as users

  from cumulative_app_users
)

select date
  , total::int as total_registered_users
  , app::int as total_registered_app_users
  , country

from total
  pivot (sum(users) for type in ('Total', 'App'))

order by 1