
    
    

select
    (user_uuid || account_list ) as unique_field,
    count(*) as n_records

from "dev"."customers"."users"
where (user_uuid || account_list ) is not null
group by (user_uuid || account_list )
having count(*) > 1


