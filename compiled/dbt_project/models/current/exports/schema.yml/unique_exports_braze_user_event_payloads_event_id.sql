
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "dev"."exports"."exports_braze_user_event_payloads"
where event_id is not null
group by event_id
having count(*) > 1


