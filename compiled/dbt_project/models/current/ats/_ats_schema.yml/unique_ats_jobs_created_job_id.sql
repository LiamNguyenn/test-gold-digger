
    
    

select
    job_id as unique_field,
    count(*) as n_records

from "dev"."ats"."jobs_created"
where job_id is not null
group by job_id
having count(*) > 1


