
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."organics"."organisations"
where id is not null
group by id
having count(*) > 1


