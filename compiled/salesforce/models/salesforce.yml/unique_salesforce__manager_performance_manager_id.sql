
    
    

select
    manager_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."salesforce__manager_performance"
where manager_id is not null
group by manager_id
having count(*) > 1


