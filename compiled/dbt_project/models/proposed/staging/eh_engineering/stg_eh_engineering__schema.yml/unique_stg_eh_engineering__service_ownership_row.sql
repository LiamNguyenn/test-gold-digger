
    
    

select
    row as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_eh_engineering__service_ownership"
where row is not null
group by row
having count(*) > 1


