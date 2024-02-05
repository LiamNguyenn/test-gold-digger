
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."stg_salesforce__event"
where event_id is not null
group by event_id
having count(*) > 1


