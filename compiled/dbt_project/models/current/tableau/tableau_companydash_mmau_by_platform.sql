

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
    
    

    )

    select *
    from unioned
    where generated_number <= 14
    order by generated_number

)
        where
            "date" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
            
              and date > (select max(date) from "dev"."tableau"."companydash_mmau_by_platform" )
            
    )

select
    dates.date,
    (
        case
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
    ) as country,
    count(
        distinct case when app_version_string is not null then coalesce(e.user_id, e.user_email) end
    ) as mobile_app_mau,
    count(distinct case when app_version_string is null then coalesce(e.user_id, e.user_email) end) as web_browser_mau,
    count(
        distinct case when app_version_string is not null and o.is_paying_eh then coalesce(e.user_id, e.user_email) end
    ) as mobile_app_mmau,
    count(
        distinct case when app_version_string is null and o.is_paying_eh then coalesce(e.user_id, e.user_email) end
    ) as web_browser_mmau
from dates

inner join "dev"."customers"."events" as e 
    on e.timestamp < dateadd(day, 1, dates.date) and e.timestamp >= dateadd(day, -89, dates.date)

left join "dev"."employment_hero"."employees" as ee 
    on ee.uuid = e.member_uuid

left join "dev"."employment_hero"."organisations" as o 
    on ee.organisation_id = o.id

left join "dev"."eh_product"."module_ownership" as mo 
    on mo.event_module = e.module

group by 1, 2