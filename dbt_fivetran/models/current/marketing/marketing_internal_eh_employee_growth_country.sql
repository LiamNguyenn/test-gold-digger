{{ config(materialized="view", alias="internal_eh_employee_growth_country") }}

with
    valid_members as (
        select *
        from {{ ref('employment_hero_employees') }}
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
                    dateadd('month', 1 - n, date_trunc('month', getdate()))::date
                    as month
                from
                    (
                        select row_number() over () as n
                        from {{ source('postgres_public', 'members') }}
                        limit 100
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
