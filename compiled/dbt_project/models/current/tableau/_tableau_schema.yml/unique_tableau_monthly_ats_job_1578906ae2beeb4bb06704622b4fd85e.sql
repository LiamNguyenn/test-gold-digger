
    
    

select
    (industry || month_posted || country || is_test_job || is_remote_job) as unique_field,
    count(*) as n_records

from "dev"."tableau"."monthly_ats_jobs_posted_industry"
where (industry || month_posted || country || is_test_job || is_remote_job) is not null
group by (industry || month_posted || country || is_test_job || is_remote_job)
having count(*) > 1


