
    
    

select
    sla_event_id as unique_field,
    count(*) as n_records

from "dev"."zendesk"."zendesk__sla_policies"
where sla_event_id is not null
group by sla_event_id
having count(*) > 1


