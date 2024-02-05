
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_postgres_public__user_infos"
where id is not null
group by id
having count(*) > 1


