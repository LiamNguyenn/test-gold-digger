
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."sales"."csat"
where id is not null
group by id
having count(*) > 1


