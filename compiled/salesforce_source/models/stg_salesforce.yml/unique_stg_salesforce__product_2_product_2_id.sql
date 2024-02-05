
    
    

select
    product_2_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."stg_salesforce__product_2"
where product_2_id is not null
group by product_2_id
having count(*) > 1


