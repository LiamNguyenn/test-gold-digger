
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_workshop_public__prometheus_service_stats"
where id is not null
group by id
having count(*) > 1


