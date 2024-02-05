
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_herodollar_service_public__tracking_infos"
where id is not null
group by id
having count(*) > 1


