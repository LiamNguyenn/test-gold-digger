
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."tableau"."tableau_salary_range"
where id is not null
group by id
having count(*) > 1


