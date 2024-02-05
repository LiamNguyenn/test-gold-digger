
    
    

select
    host_member_id as unique_field,
    count(*) as n_records

from "dev"."employment_hero"."_v_gt_employees"
where host_member_id is not null
group by host_member_id
having count(*) > 1


