
    
    

select
    eh_platform_user_id as unique_field,
    count(*) as n_records

from "dev"."marketing"."users_from_snapshot"
where eh_platform_user_id is not null
group by eh_platform_user_id
having count(*) > 1


