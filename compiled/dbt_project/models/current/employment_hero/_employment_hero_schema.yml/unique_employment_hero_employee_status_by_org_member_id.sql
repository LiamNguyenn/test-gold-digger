
    
    

select
    member_id as unique_field,
    count(*) as n_records

from "dev"."employment_hero"."employee_status_by_org"
where member_id is not null
group by member_id
having count(*) > 1


