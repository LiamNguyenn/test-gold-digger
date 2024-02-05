
    
    

select
    opportunity_line_item_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."salesforce__opportunity_line_item_enhanced"
where opportunity_line_item_id is not null
group by opportunity_line_item_id
having count(*) > 1


