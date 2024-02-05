
    
    

select
    job_boards_post_id as unique_field,
    count(*) as n_records

from "dev"."ats"."jobs_posted"
where job_boards_post_id is not null
group by job_boards_post_id
having count(*) > 1


