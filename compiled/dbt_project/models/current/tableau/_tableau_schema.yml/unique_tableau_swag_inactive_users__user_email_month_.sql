
    
    

select
    (user_email || month) as unique_field,
    count(*) as n_records

from "dev"."tableau"."swag_inactive_users"
where (user_email || month) is not null
group by (user_email || month)
having count(*) > 1


