

with
    pass_fail_probation as (
        select
            date_trunc('month', start_date) as month_start,
            job_sector,
            job_apps.country,
            datediff(
                'month', start_date, coalesce(termination_date, getdate())
            ) as month_diff,
            case
                when month_diff <= 6 then 'Failed Probation' else 'Passed Probation'
            end as probation_pass_fail,
            emp.organisation_id,
            portal_name
        from "dev"."employment_hero"."employees" emp
        left join
            "dev"."ats"."job_applications" job_apps
            on (
                emp.organisation_id = job_apps.organisation_id
                and emp.email = job_apps.applicant_email
            )
        where
            emp.user_id not in (
                select user_id
                from employment_hero.employees emp
                group by user_id, organisation_id
                having count(*) > 1
            )
    ),
    pass_fail_probation_table as (

        select
            month_start,
            job_sector,
            country,
            portal_name,
            company_size,
            probation_pass_fail,
            count(*)
        from pass_fail_probation pfp
        left join
            "dev"."tableau"."v_org_size" as os
            on (pfp.organisation_id = os.organisation_id and pfp.month_start = os.date)
        where month_start >= '2021-01-01'
        group by 1, 2, 3, 4, 5, 6
        order by 1, 2, 3, 4, 5, 6
    )
select
    *,
    md5(cast(coalesce(cast(month_start as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(country as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(job_sector as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(portal_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(company_size as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(probation_pass_fail as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as surrogate_key
from pass_fail_probation_table