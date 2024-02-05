
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_github__repository"
where id is not null
group by id
having count(*) > 1


