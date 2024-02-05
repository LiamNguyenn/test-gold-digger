
    
    

select
    transaction_id as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."instapay_transactions_with_member_profile"
where transaction_id is not null
group by transaction_id
having count(*) > 1


