
    
    

select
    candidate_job_id as unique_field,
    count(*) as n_records

from "dev"."ats"."job_application_profile"
where candidate_job_id is not null
group by candidate_job_id
having count(*) > 1


