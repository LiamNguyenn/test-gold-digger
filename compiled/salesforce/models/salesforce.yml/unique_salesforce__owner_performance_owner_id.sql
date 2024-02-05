
    
    

select
    owner_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."salesforce__owner_performance"
where owner_id is not null
group by owner_id
having count(*) > 1


