

with
    valid_members as (
        select *
        from "dev"."employment_hero"."employees"
        where
            (termination_date is null or termination_date >= created_at)
            and organisation_id = 8701
    ),
    member_creations as (
        select
            date_trunc('month', created_at) as creation_date,
            work_country,
            count(*) as new_emps
        from valid_members
        group by 1, 2
        order by creation_date desc
    ),
    terminations as (
        select
            date_trunc(
                'month', coalesce(termination_date, created_at)
            ) as terminated_date,
            work_country,
            coalesce(count(*), 0) as terminated_emps
        from valid_members
        where not active
        group by 1, 2
        order by terminated_date desc
    ),
    dates as (
        select *
        from (select distinct work_country from valid_members)
        cross join
            (
                select
                    dateadd('month', 1 - generated_number::int, date_trunc('month', getdate()))::date
                    as month
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
    where generated_number <= 100
    order by generated_number

)
            )
        order by work_country, month
    ),

    combined as (
        select d.month, d.work_country, new_emps, terminated_emps
        from dates d
        left join member_creations m on d.month = m.creation_date and d.work_country = m.work_country
        left join terminations t on d.month = terminated_date and d.work_country = t.work_country
        order by month
    ),

    running_total as (
        select
            work_country,
            month,
            COALESCE(new_emps, 0) as new_emps,
            coalesce(terminated_emps, 0) as terminated_emps,
            COALESCE(sum(new_emps - coalesce(terminated_emps, 0)) over (
                partition by work_country
                order by month
                rows between unbounded preceding and current row
            ), 0) as runningtotal
        from combined
    )

select *
from running_total