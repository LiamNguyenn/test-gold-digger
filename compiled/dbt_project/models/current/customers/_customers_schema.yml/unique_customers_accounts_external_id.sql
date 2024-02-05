
    
    

select
    external_id as unique_field,
    count(*) as n_records

from "dev"."customers"."accounts"
where external_id is not null
group by external_id
having count(*) > 1


