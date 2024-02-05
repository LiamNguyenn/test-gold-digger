
    
    



select (industry || month_posted || country || is_test_job || is_remote_job)
from "dev"."tableau"."monthly_ats_jobs_posted_industry"
where (industry || month_posted || country || is_test_job || is_remote_job) is null


