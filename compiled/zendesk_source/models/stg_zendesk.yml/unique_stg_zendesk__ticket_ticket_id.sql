
    
    

select
    ticket_id as unique_field,
    count(*) as n_records

from "dev"."zendesk"."stg_zendesk__ticket"
where ticket_id is not null
group by ticket_id
having count(*) > 1


