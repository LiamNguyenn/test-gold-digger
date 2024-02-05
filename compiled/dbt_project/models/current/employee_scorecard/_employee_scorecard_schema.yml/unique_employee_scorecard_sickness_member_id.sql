
    
    

select
    member_id as unique_field,
    count(*) as n_records

from "dev"."employee_scorecard"."sickness_scorecard"
where member_id is not null
group by member_id
having count(*) > 1


