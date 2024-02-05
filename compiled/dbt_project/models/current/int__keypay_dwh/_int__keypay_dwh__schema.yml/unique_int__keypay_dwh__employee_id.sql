
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."int__keypay_dwh"."employee"
where id is not null
group by id
having count(*) > 1


