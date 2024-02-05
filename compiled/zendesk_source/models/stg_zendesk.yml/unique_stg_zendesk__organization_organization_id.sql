
    
    

select
    organization_id as unique_field,
    count(*) as n_records

from "dev"."zendesk"."stg_zendesk__organization"
where organization_id is not null
group by organization_id
having count(*) > 1


