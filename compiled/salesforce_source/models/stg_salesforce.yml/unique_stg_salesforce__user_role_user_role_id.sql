
    
    

select
    user_role_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."stg_salesforce__user_role"
where user_role_id is not null
group by user_role_id
having count(*) > 1


