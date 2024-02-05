
    
    

select
    opportunity_id as unique_field,
    count(*) as n_records

from "dev"."sales"."closed_lost_opportunities"
where opportunity_id is not null
group by opportunity_id
having count(*) > 1


