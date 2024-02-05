
    
    

select
    uuid as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_postgres_public__users"
where uuid is not null
group by uuid
having count(*) > 1


