
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_workshop_public__source_usages"
where id is not null
group by id
having count(*) > 1

