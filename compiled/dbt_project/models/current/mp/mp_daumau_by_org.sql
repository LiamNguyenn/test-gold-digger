

with
dates as (
    select dateadd('day', -generated_number::int, (current_date + 1)) as date
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
    where date < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
               
      and date > (select max(date) from "dev"."mp"."daumau_by_org")
  
),

sessions as (
    select distinct
        date as session_date,
        m.organisation_id,
        m.id as member_id
    from
        "dev"."mp"."daily_members" as d
    inner join "dev"."postgres_public"."members" as m
        on
            d.member_id = m.id
    inner join "dev"."postgres_public"."users" as u
        on
            m.user_id = u.id
    where
        u.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
        and not m.system_manager
        and not m.system_user
        and not m._fivetran_deleted
        and not m.is_shadow_data
        and not u.is_shadow_data
        and date < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")

),

dau as (
    select
        d.date,
        organisation_id,
        count(*) as daily_users
    from
        dates as d
    left join sessions as s
        on
            d.date = s.session_date
    group by
        d.date,
        organisation_id
),

mau as (
    select
        d.date,
        s.organisation_id,
        count(distinct s.member_id) as monthly_users
    from
        dates as d
    left join sessions as s
        on
            s.session_date < dateadd(day, 1, d.date)
            and s.session_date > dateadd(day, -29, d.date)
    group by
        d.date,
        s.organisation_id
)

select
    mau.date,
    mau.organisation_id,
    coalesce(daily_users, 0)                                   as daily_users,
    coalesce(monthly_users, 0)                                 as monthly_users,
    coalesce(daily_users, 0) / nullif(monthly_users, 0)::float as dau_mau
from
    mau
left join dau on
    mau.date = dau.date
    and mau.organisation_id = dau.organisation_id