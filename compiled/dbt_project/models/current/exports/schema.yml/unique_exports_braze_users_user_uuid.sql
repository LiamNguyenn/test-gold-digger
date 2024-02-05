
    
    

select
    user_uuid as unique_field,
    count(*) as n_records

from "dev"."exports"."exports_braze_users"
where user_uuid is not null
group by user_uuid
having count(*) > 1


