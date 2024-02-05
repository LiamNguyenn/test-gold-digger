
    
    

select
    dim_user_sk as unique_field,
    count(*) as n_records

from "dev"."mart"."dim_user"
where dim_user_sk is not null
group by dim_user_sk
having count(*) > 1


