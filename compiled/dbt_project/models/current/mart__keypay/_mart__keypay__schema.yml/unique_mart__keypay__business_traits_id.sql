
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."mart__keypay"."business_traits"
where id is not null
group by id
having count(*) > 1


