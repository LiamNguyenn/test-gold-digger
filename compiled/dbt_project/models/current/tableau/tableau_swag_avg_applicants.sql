

with
    avg_applicants as (
        select
            date_trunc('month', posted_at) as month_posted,
            country,
            job_sector,
            portal_name,
            company_size,
            avg(applicants_count)
        from "dev"."ats"."jobs_posted" as jp
        left join
            "dev"."tableau"."v_org_size" as os
            on (
                jp.organisation_id = os.organisation_id
                and date_trunc('month', posted_at) = os.date
            )
        group by 1, 2, 3, 4, 5
        order by 1, 2, 3, 4, 5
    )
select
    *,
    md5(cast(coalesce(cast(month_posted as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(country as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(job_sector as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(portal_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(company_size as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as surrogate_key
from avg_applicants