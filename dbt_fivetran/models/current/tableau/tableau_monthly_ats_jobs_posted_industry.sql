{{ config(alias="monthly_ats_jobs_posted_industry") }}

select
    industry,
    date_trunc('month', posted_at) as month_posted,
    country,
    case when is_test_job then 'True' else 'False' end as is_test_job,
    case when is_remote_job then 'True' else 'False' end as is_remote_job,
    count(*)
from ats.jobs_posted
where industry is not null and country is not null
group by 1, 2, 3, 4, 5
order by industry, month_posted
