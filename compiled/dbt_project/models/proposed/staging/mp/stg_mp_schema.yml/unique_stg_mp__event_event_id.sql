
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_mp__event"
where event_id is not null
group by event_id
having count(*) > 1


