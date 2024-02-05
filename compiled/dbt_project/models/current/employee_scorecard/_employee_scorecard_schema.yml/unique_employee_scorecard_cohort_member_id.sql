
    
    

select
    member_id as unique_field,
    count(*) as n_records

from "dev"."employee_scorecard"."employee_scorecard_cohort"
where member_id is not null
group by member_id
having count(*) > 1


