
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_checkly__all_checks_results"
where id is not null
group by id
having count(*) > 1


