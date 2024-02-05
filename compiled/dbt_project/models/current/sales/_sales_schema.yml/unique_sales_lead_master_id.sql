
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."sales"."lead_master"
where id is not null
group by id
having count(*) > 1


