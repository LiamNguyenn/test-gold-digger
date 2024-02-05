
    
    

select
    ticket_comment_id as unique_field,
    count(*) as n_records

from "dev"."zendesk"."stg_zendesk__ticket_comment"
where ticket_comment_id is not null
group by ticket_comment_id
having count(*) > 1


