
    
    

select
    member_uuid as unique_field,
    count(*) as n_records

from "dev"."tableau"."tableau_companydash_eh_members_managers"
where member_uuid is not null
group by member_uuid
having count(*) > 1


