
    
    

select
    account_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."stg_salesforce__account"
where account_id is not null
group by account_id
having count(*) > 1


