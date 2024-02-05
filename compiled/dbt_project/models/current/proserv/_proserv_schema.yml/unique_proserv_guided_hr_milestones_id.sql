
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."proserv"."guided_hr_milestones"
where id is not null
group by id
having count(*) > 1


