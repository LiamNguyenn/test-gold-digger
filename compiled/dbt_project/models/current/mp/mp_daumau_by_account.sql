

with
    dates as (
        select dateadd('day', -generated_number::int, (current_date + 1)) date
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
        where
            "date" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
             and date > (select max(date) from "dev"."mp"."daumau_by_account") 
    ),
    account_events as (
        select e.user_id, e.timestamp, u.account_list as account_id
        from "dev"."customers"."events" as e
        join "dev"."customers"."users" u on u.user_uuid = e.user_id
        -- TODO: Fix circular reference with customers.accounts
        join "dev"."salesforce"."account" a on u.account_list = a.id
        where e.timestamp < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
    ),
    account_dau as (
        select date_trunc('day', e.timestamp) as "date", e.account_id, count(distinct e.user_id) as daily_users
        from account_events as e
        group by 1, 2
    ),
    account_mau as (
        select dates.date, e.account_id, count(distinct e.user_id) as monthly_users
        from dates
        join
            account_events as e
            on e.timestamp < dateadd(day, 1, dates.date)
            and e.timestamp > dateadd(day, -29, dates.date)
        group by 1, 2
    )

select
    account_mau.date,
    account_mau.account_id,
    coalesce(daily_users, 0) as daily_users,
    coalesce(monthly_users, 0) as monthly_users,
    coalesce(daily_users, 0) / nullif(monthly_users, 0)::float as dau_mau
from account_mau
left join account_dau on account_mau.date = account_dau.date and account_mau.account_id = account_dau.account_id