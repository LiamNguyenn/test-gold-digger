
    
    

select
    opportunity_line_item_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."stg_salesforce__opportunity_line_item"
where opportunity_line_item_id is not null
group by opportunity_line_item_id
having count(*) > 1


