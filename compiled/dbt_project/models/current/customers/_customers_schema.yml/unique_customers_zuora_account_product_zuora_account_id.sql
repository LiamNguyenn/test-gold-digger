
    
    

select
    zuora_account_id as unique_field,
    count(*) as n_records

from "dev"."customers"."zuora_account_product"
where zuora_account_id is not null
group by zuora_account_id
having count(*) > 1


