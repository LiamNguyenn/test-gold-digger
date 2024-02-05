
    
    

select
    ticket_id as unique_field,
    count(*) as n_records

from "dev"."zendesk"."zendesk__ticket_metrics"
where ticket_id is not null
group by ticket_id
having count(*) > 1


