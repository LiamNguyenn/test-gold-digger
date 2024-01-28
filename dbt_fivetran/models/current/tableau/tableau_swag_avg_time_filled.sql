{{ config(alias="swag_avg_time_filled") }}

with
    avg_time_filled as (

        select
            date_trunc('month', posted_at) as month_posted,
            country,
            job_sector,
            portal_name,
            company_size,
            avg(time_to_hire) as average_time_to_hire
        from {{ ref("ats_job_applications") }} job_apps
        left join
            {{ ref("tableau_v_org_size") }} as os
            on (
                job_apps.organisation_id = os.organisation_id
                and date_trunc('month', posted_at) = os.date
            )
        where result = 'hired'
        group by 1, 2, 3, 4, 5
        order by 1, 2, 3, 4, 5
    )
select
    *,
    {{
        dbt_utils.generate_surrogate_key(
            ["month_posted", "country", "job_sector", "portal_name", "company_size"]
        )
    }} as surrogate_key
from avg_time_filled
