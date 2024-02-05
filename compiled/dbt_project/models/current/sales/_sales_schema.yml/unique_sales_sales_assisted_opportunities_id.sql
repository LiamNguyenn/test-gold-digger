
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."sales"."sales_assisted_opportunities"
where id is not null
group by id
having count(*) > 1


