
    
    

select
    member_id as unique_field,
    count(*) as n_records

from "dev"."salary_guide"."eh_paying_employee_job_titles"
where member_id is not null
group by member_id
having count(*) > 1


