
    
    

select
    contact_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."stg_salesforce__contact"
where contact_id is not null
group by contact_id
having count(*) > 1


