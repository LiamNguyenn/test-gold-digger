
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_heroshop_db_public__orders"
where id is not null
group by id
having count(*) > 1

