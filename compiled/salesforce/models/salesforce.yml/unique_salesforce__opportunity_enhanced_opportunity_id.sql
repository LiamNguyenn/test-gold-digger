
    
    

select
    opportunity_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."salesforce__opportunity_enhanced"
where opportunity_id is not null
group by opportunity_id
having count(*) > 1


