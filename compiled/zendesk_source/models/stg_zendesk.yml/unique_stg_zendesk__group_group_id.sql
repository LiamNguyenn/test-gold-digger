
    
    

select
    group_id as unique_field,
    count(*) as n_records

from "dev"."zendesk"."stg_zendesk__group"
where group_id is not null
group by group_id
having count(*) > 1


