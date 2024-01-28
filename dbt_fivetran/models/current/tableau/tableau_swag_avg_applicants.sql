{{ config(alias="swag_avg_applicants") }}

with
    avg_applicants as (
        select
            date_trunc('month', posted_at) as month_posted,
            country,
            job_sector,
            portal_name,
            company_size,
            avg(applicants_count)
        from {{ ref("ats_jobs_posted") }} as jp
        left join
            {{ ref("tableau_v_org_size") }} as os
            on (
                jp.organisation_id = os.organisation_id
                and date_trunc('month', posted_at) = os.date
            )
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
from avg_applicants
