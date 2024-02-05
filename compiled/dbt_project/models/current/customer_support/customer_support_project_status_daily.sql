with
dates as (
    select distinct dateadd('day', -generated_number::int, current_date) as date  -- noqa: RF04
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
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
    
    
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
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 3000
    order by generated_number

)
),

project_status_history as (
    select distinct
        ip.id                                                                                          as project_id,
        (
            case
                when lower(a.geo_code_c) = 'au'
                    then 'Australia'
                when lower(a.geo_code_c) = 'uk'
                    then 'United Kingdom'
                when lower(a.geo_code_c) = 'sg'
                    then 'Singapore'
                when lower(a.geo_code_c) = 'my'
                    then 'Malaysia'
                when lower(a.geo_code_c) = 'nz'
                    then 'New Zealand'
                else 'untracked'
            end
        )
        as country,
        ip.service_offering_c                                                                          as service_offering,
        iph.created_date::date                                                                         as agg_date,
        iph.new_value,
        row_number() over (partition by iph.id, iph.created_date::date order by iph.created_date desc) as rn
    from
        "dev"."salesforce"."implementation_project_history" as iph  -- noqa: AL06
    inner join "dev"."salesforce"."implementation_project_c" as ip  -- noqa: AL06
        on
            iph.parent_id = ip.id
            and iph.created_date >= '2019-01-01'
            and ip.created_date >= '2019-01-01'
    left join "dev"."salesforce"."account" as a on ip.account_c = a.id  -- noqa: AL06
    where
        iph.field = 'Status__c'
-- and ip.id = 'a0B5h000002EjcnEAC'
),

min_max_project as (
    select
        project_id,
        min(agg_date) as min_agg_date
    from
        project_status_history
    group by project_id
),

project_over_time_w_status as (
    select
        *,
        last_value(country ignore nulls) over (partition by project_id order by date rows unbounded preceding)          as country_c,
        last_value(service_offering ignore nulls) over (partition by project_id order by date rows unbounded preceding) as service_offering_c,
        last_value(value ignore nulls) over (partition by project_id order by date rows unbounded preceding)            as status
    from
        (
            select
                mmp.project_id,
                d.*,
                psh.new_value as value,  -- noqa: RF04
                psh.country,
                psh.service_offering
            from
                dates as d  -- noqa: AL06
            inner join min_max_project as mmp  -- noqa: AL06
                on
                    d.date >= mmp.min_agg_date
                    and d.date <= current_date
            left join project_status_history as psh  -- noqa: AL06
                on
                    d.date = psh.agg_date
                    and mmp.project_id = psh.project_id
                    and psh.rn = 1
            order by
                mmp.project_id asc
        )
)

select
    date::date                                                                                        as date,  -- noqa: RF04
    coalesce(country_c, 'untracked')                                                                  as country,
    coalesce(service_offering_c, 'untracked')                                                         as service_offering,
    count(case when status in ('On-Hold') then project_id end)                                        as on_hold_projects,
    count(case when status in ('Off track', 'Delayed', 'At risk') then project_id end)                as red_flag_projects,
    count(case when status in ('Active', 'New', 'On track') then project_id end)                      as in_progress_projects,
    count(case when status in ('Closed', 'Live', 'Delivered', 'Completed', 'CS') then project_id end) as completed_projects,
    count(case when status in ('Churned', 'Expired') then project_id end)                             as churned_projects,
    count(case when status is NULL then project_id end)                                               as null_projects
from
    project_over_time_w_status
where
    date >= '2019-01-01'::date
group by
    date,
    country_c,
    service_offering_c
order by
    date desc